#!/usr/bin/env python3
"""
NxReduce Job Submission App
Stand-alone Flask app for generating and submitting nxreduce cluster jobs.
Usage:  python nxjobs_app.py
"""

from flask import Flask, render_template_string, request, jsonify, send_file, Response
import os, datetime, subprocess, threading, time

app = Flask(__name__)

# ── Short-lived caches to avoid repeated expensive filesystem/h5py work ────────
# Raw-scan results and .nxs stage detection get called multiple times per request
# cycle (preview + generate) and on every watcher refresh. Cache for a few seconds
# so repeats are instant.
_CACHE_TTL = 8.0           # seconds
_nxs_cache = {}            # nxs_path -> (expiry, signature, (done, note))
_cache_lock = threading.Lock()

# ── Root for directory browser ────────────────────────────────────────────────
ROOT = "/nfs/chess/id4baux/2026-3"

# ── CHESS logo ────────────────────────────────────────────────────────────────
CHESS_LOGO = "/nfs/chess/id4baux/chesslogo.png"

# ── In-memory job history (cleared on restart) ────────────────────────────────
_job_history = []
_history_lock = threading.Lock()

# ── Auto-Watch state: which temps have been auto-handled, keyed "temp:stage" ───
# stage is "noparent" or "parent". Ensures each temp is auto-processed once per
# stage. Also keeps a small activity log for the UI.
_autowatch_handled = set()
_autowatch_log = []          # list of {time, temp, stage, action, ok, detail}
_autowatch_lock = threading.Lock()

# ── Auto-Submit state: a SEPARATE parent-aware auto-submitter that skips the ───
# parent temperature (default 300). Independent handled set + log from Auto-Watch
# above, so starting/stopping/resetting one does not affect the other.
_autosubmit_handled = set()
_autosubmit_log = []
_autosubmit_lock = threading.Lock()

# ── Auto-Submit job tracking: remember the qsub job id we submitted per key ────
# (key = "USER_DIR|temp:stage"). Used to detect a job that was cancelled with
# `qdel` (or died) without producing output, so Auto-Submit can resubmit that
# temperature. Shares _autosubmit_lock.
_autosubmit_jobs = {}        # key -> job_id string

# Short cache of the cluster's active job ids (qstat), shared across temps in a
# single poll so we don't shell out once per temperature.
_qstat_cache = {"expiry": 0.0, "ids": set()}
_qstat_lock = threading.Lock()

# Short cache of the cluster's active job *names* (full SGE job names, any user),
# so an auto-poller can tell that the same script was already submitted — e.g.
# by hand from lnx16 — and skip it instead of submitting a duplicate.
_qstat_names_cache = {"expiry": 0.0, "names": set()}
_qstat_names_lock = threading.Lock()

# Short cache of jobs SGE reports in a problem state (error / held / suspended),
# parsed from `qstat -r -u '*'`. Shared by the "Jobs needing attention" table so
# repeated refreshes don't shell out to qstat every time.
_problem_cache = {"expiry": 0.0, "jobs": None}
_problem_lock = threading.Lock()

# Job ids THIS app submitted (from any panel — manual submit, Auto-Scan,
# Auto-Watch, Auto-Submit — and under any account, local or remote). The "Jobs
# needing attention" table is limited to these so it only ever shows the user's
# own nxrefine jobs, never other people's cluster jobs.
_submitted_job_ids = set()
_submitted_lock = threading.Lock()


def _record_submitted(job_id):
    """Remember a job id we just submitted, so job-health can scope to our jobs."""
    if not job_id:
        return
    with _submitted_lock:
        _submitted_job_ids.add(str(job_id).strip())


# ─────────────────────────────────────────────────────────────────────────────
# Remote submission: submit `qsub` as another user over SSH.
#
# On a shared cluster each account has its own job-submission quota, so letting a
# person submit under THEIR OWN login spreads jobs across accounts the way the
# cluster intends. The generated scripts already live on the shared
# /nfs/chess/... filesystem, so a remote host runs the IDENTICAL `qsub <script>`
# path — nothing needs to be copied.
#
# SECURITY: the password (if any) is read from the form, held only in memory for
# the duration of the submission, and is NEVER written to disk, NEVER placed on a
# command line (paramiko takes it as an argument; the sshpass fallback passes it
# via the SSHPASS environment variable, not argv), and NEVER logged or echoed in
# any result shown to the user.
# ─────────────────────────────────────────────────────────────────────────────
class _RunResult:
    """Minimal stand-in for subprocess.CompletedProcess so existing call sites
    can keep using .returncode / .stdout / .stderr unchanged."""
    __slots__ = ("returncode", "stdout", "stderr")
    def __init__(self, returncode, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _ssh_cfg_from_form(form):
    """Return an SSH submission config dict, or None for ordinary local submit.

    Only returns a config when remote submission is explicitly enabled AND a
    username is supplied. A blank password means "use SSH keys / agent"."""
    try:
        enabled = (form.get("remote_enable", "0") or "0").strip() == "1"
    except Exception:
        return None
    if not enabled:
        return None
    user = (form.get("remote_user", "") or "").strip()
    if not user:
        return None
    host = (form.get("remote_host", "") or "").strip() or "lnx201.classe.cornell.edu"
    password = form.get("remote_pass", "") or ""     # usually blank on 2FA clusters
    key = (form.get("remote_key", "") or "").strip()  # optional private-key path
    # On a Duo/2FA cluster a plain password can't complete the login, so SSH-key
    # auth is the intended path: leave the password blank and (optionally) point
    # at a private key, or rely on the default ~/.ssh keys / ssh-agent.
    return {"host": host, "user": user, "password": password, "key": key}


def _run_qsub(qcmd, ssh_cfg=None, timeout=30):
    """Run a qsub command either locally (ssh_cfg is None — original behavior) or
    on a remote host as another user (ssh_cfg from _ssh_cfg_from_form). Returns an
    object exposing .returncode / .stdout / .stderr like subprocess.run.

    Local submission may raise FileNotFoundError when the SGE environment isn't
    loaded; callers already handle that. Remote failures are returned as a
    non-zero result with an explanatory stderr (never raised)."""
    if not ssh_cfg:
        return subprocess.run(qcmd.split(), capture_output=True, text=True, timeout=timeout)

    host = ssh_cfg["host"]
    user = ssh_cfg["user"]
    password = ssh_cfg.get("password") or None
    key = ssh_cfg.get("key") or None

    # 1) Preferred: paramiko (pure-Python; password never touches a command line).
    try:
        import paramiko
    except Exception:
        paramiko = None
    if paramiko is not None:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ck = {"hostname": host, "username": user, "timeout": timeout}
            if key:
                # Explicit private key (+ agent/default keys as backup). This is
                # the path that works on a 2FA cluster — no password, no Duo prompt.
                ck.update(key_filename=key, look_for_keys=True, allow_agent=True)
            elif password:
                ck.update(password=password, look_for_keys=False, allow_agent=False)
            else:
                # No key path given and no password: use ssh-agent / default
                # ~/.ssh keys. Still key-based, so no Duo prompt.
                ck.update(look_for_keys=True, allow_agent=True)
            client.connect(**ck)
            _in, _out, _err = client.exec_command(qcmd, timeout=timeout)
            out = _out.read().decode("utf-8", "replace")
            err = _err.read().decode("utf-8", "replace")
            rc = _out.channel.recv_exit_status()
            return _RunResult(rc, out, err)
        except Exception as ex:
            return _RunResult(255, "", f"SSH to {user}@{host} failed: {ex}")
        finally:
            try:
                client.close()
            except Exception:
                pass

    # 2) Fallback: the system ssh client. Key auth (or ssh-agent) is used unless a
    #    password is supplied, in which case sshpass passes it via the SSHPASS
    #    environment variable (never argv).
    base = ["-o", "StrictHostKeyChecking=accept-new",
            "-o", f"ConnectTimeout={int(timeout)}"]
    if key:
        base += ["-i", key]
    try:
        if password and not key:
            env = dict(os.environ, SSHPASS=password)
            cmd = ["sshpass", "-e", "ssh"] + base + [f"{user}@{host}", qcmd]
            return subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout + 15, env=env)
        # Key / agent auth — BatchMode so it fails fast instead of prompting.
        cmd = ["ssh", "-o", "BatchMode=yes"] + base + [f"{user}@{host}", qcmd]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 15)
    except FileNotFoundError:
        return _RunResult(255, "",
            "Remote submission needs the Python 'paramiko' module (or the system "
            "'ssh' command, plus 'sshpass' only for password login). On this 2FA "
            "cluster, set up an SSH key and leave the password blank.")
    except Exception as ex:
        return _RunResult(255, "", f"SSH to {user}@{host} failed: {ex}")


# ─────────────────────────────────────────────────────────────────────────────
# Requeue detection: if a submitted job is cancelled (`qdel -f <id>`) or dies
# without processing a temperature, Auto-Submit should pick that temperature up
# again instead of treating it as permanently handled.
# ─────────────────────────────────────────────────────────────────────────────
def _extract_job_id(text):
    """Pull the numeric SGE job id out of qsub stdout, e.g.
    'Your job 1234567 ("name") has been submitted' -> '1234567'."""
    import re
    m = re.search(r"\b(\d{3,})\b", text or "")
    return m.group(1) if m else None


def _active_job_ids(ttl=5.0):
    """Set of job-id strings currently known to SGE (any state) via
    `qstat -u '*'`, cached briefly. Returns None if qstat is unavailable, which
    the resubmit check treats conservatively (do NOT resubmit) to avoid
    duplicate submissions."""
    now = time.time()
    with _qstat_lock:
        if now < _qstat_cache["expiry"]:
            return set(_qstat_cache["ids"])
    ids = set()
    ok = False
    try:
        r = subprocess.run(["qstat", "-u", "*"], capture_output=True, text=True, timeout=15)
        ok = (r.returncode == 0)
        for line in (r.stdout or "").splitlines():
            parts = line.split()
            if parts and parts[0].isdigit():
                ids.add(parts[0])
    except Exception:
        ok = False
    if not ok:
        return None
    with _qstat_lock:
        _qstat_cache["ids"] = set(ids)
        _qstat_cache["expiry"] = now + ttl
    return ids


def _temp_has_output(user_dir, temp):
    """Heuristic: has this temperature folder actually been processed? True if an
    .nxs file (nxreduce output) exists in the temp folder. A cancelled/killed job
    that never processed the temperature leaves no such output."""
    d = os.path.join(user_dir, str(temp))
    try:
        for f in os.listdir(d):
            if f.endswith(".nxs"):
                return True
    except Exception:
        pass
    return False


def _should_resubmit(user_dir, temp, job_id, active_ids):
    """Decide whether an already-handled temp needs resubmitting because its job
    was qdel'd / died without producing output. Conservative: only True when we
    tracked a job id, qstat is available (active_ids is not None), the job is no
    longer in the queue, AND the temp has no processed output yet."""
    if not job_id:
        return False
    if active_ids is None:            # qstat unavailable -> stay safe
        return False
    if str(job_id) in active_ids:     # still queued/running -> leave it
        return False
    return not _temp_has_output(user_dir, temp)


def _active_job_names(ttl=5.0):
    """Set of full SGE job names currently in the queue (ANY user, any state),
    read with `qstat -r -u '*'` which prints a `Full jobname:` line per job.

    The plain `qstat` name column is truncated to ~10 chars, so two temps of the
    same sample (e.g. hallas-S11-30 and hallas-S11-300) collapse to the same
    string — useless for matching. `-r` gives the untruncated name. Cached
    briefly and shared across temps in one poll. Returns None if qstat is
    unavailable, which callers treat conservatively (no dedupe, but also no false
    skip)."""
    now = time.time()
    with _qstat_names_lock:
        if now < _qstat_names_cache["expiry"]:
            return set(_qstat_names_cache["names"])
    names = set()
    ok = False
    try:
        r = subprocess.run(["qstat", "-r", "-u", "*"],
                           capture_output=True, text=True, timeout=20)
        ok = (r.returncode == 0)
        for line in (r.stdout or "").splitlines():
            s = line.strip()
            if s.startswith("Full jobname:"):
                nm = s.split(":", 1)[1].strip()
                if nm:
                    names.add(nm)
    except Exception:
        ok = False
    if not ok:
        return None
    with _qstat_names_lock:
        _qstat_names_cache["names"] = set(names)
        _qstat_names_cache["expiry"] = now + ttl
    return names


def _job_already_active(base, active_names):
    """True if a job whose script is `base` (the _script_name() stem, no .sh) is
    already queued/running. SGE's default job name is the submitted script's
    filename, so we normalise each active name by dropping a trailing `.sh` and
    any `_<n>` overwrite-counter suffix, then compare for an exact match. Exact
    (not prefix) matching so temp 30 never masks temp 300."""
    if not base or not active_names:
        return False
    b = base.strip()
    for n in active_names:
        nm = n.strip()
        if nm.endswith(".sh"):
            nm = nm[:-3]
        nm = _re.sub(r'_\d+$', '', nm)   # strip overwrite counter (_2, _3, …)
        if nm == b:
            return True
    return False


def _classify_state(state):
    """Map an SGE state string to a problem category, or None if the job is
    healthy (running / queued-waiting / transferring).

    SGE state letters: r=running, qw=queued-waiting, t=transferring,
    d=deleting, h=hold, s/S=suspended, E=error (e.g. 'Eqw', 'Er'). A job is a
    problem only when it is in error, on hold, or suspended — those don't make
    progress on their own and usually need a person to look."""
    s = (state or "").strip()
    if not s:
        return None
    if "E" in s:
        return ("error", "SGE error state (" + s + ") — rejected/failed, won't run until cleared.")
    if "h" in s:
        return ("held", "Held (" + s + ") — waiting on a hold, not scheduled.")
    if "s" in s or "S" in s:
        return ("suspended", "Suspended (" + s + ") — paused by the scheduler/admin.")
    return None


def _parse_temp_from_name(name):
    """Best-effort: pull the temperature off the end of a job/script name like
    'clancy-ZnInSb-255' -> '255'. Returns '' if there's no trailing number."""
    m = _re.search(r'(\d+)\s*$', (name or "").strip())
    return m.group(1) if m else ""


def _problem_jobs(ttl=5.0):
    """List jobs SGE currently reports in a problem state (error / held /
    suspended), parsed from `qstat -r -u '*'` (all users, untruncated names).

    Returns a list of dicts (id, name, full_name, temp, user, state, queue,
    submit, category, issue), newest-first by job id. Returns None if qstat is
    unavailable (the caller shows that as 'qstat unavailable')."""
    now = time.time()
    with _problem_lock:
        if now < _problem_cache["expiry"]:
            cached = _problem_cache["jobs"]
            return None if cached is None else list(cached)

    jobs = []
    ok = False
    try:
        r = subprocess.run(["qstat", "-r", "-u", "*"],
                           capture_output=True, text=True, timeout=20)
        ok = (r.returncode == 0)
        cur = None
        for line in (r.stdout or "").splitlines():
            parts = line.split()
            if parts and parts[0].isdigit():
                # summary line: job-ID prior name user state date time [queue] slots
                state = parts[4] if len(parts) > 4 else ""
                cat = _classify_state(state)
                submit = (parts[5] + " " + parts[6]) if len(parts) > 6 else ""
                queue = ""
                for tok in parts[7:]:
                    if "@" in tok:
                        queue = tok
                        break
                cur = {
                    "id": parts[0],
                    "name": parts[2] if len(parts) > 2 else "",
                    "full_name": "",
                    "temp": "",
                    "user": parts[3] if len(parts) > 3 else "",
                    "state": state,
                    "queue": queue,
                    "submit": submit,
                    "category": cat[0] if cat else None,
                    "issue": cat[1] if cat else "",
                }
                if cat:
                    jobs.append(cur)
                else:
                    cur = None   # healthy job: ignore its detail lines
            elif cur is not None:
                s = line.strip()
                if s.startswith("Full jobname:"):
                    cur["full_name"] = s.split(":", 1)[1].strip()
                    cur["temp"] = _parse_temp_from_name(cur["full_name"])
    except Exception:
        ok = False

    result = jobs if ok else None
    with _problem_lock:
        _problem_cache["jobs"] = (list(result) if result is not None else None)
        _problem_cache["expiry"] = now + ttl
    return result


# ── HTML ──────────────────────────────────────────────────────────────────────
PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NxRefine Job Submission CLASSE Cluster for Quantum Materials Beamline(lnx201) · CHESS</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#f0f4f9;--bg-card:#fff;--bg-header:#0d1520;
    --text:#1e293b;--text-muted:#64748b;--text-header:#e2e8f0;
    --border:#d1d9e6;--accent:#0284c7;--accent2:#0ea5e9;
    --green:#16a34a;--green2:#22c55e;--red:#dc2626;
    --gold:#d97706;--mono:'IBM Plex Mono',monospace;
    --sans:'IBM Plex Sans',-apple-system,sans-serif;
  }
  body.dark{
    --bg:#060e1c;--bg-card:#0d1a2e;--bg-header:#020b14;
    --text:#e2e8f0;--text-muted:#94a3b8;--border:#1e3a5f;
    --accent:#38bdf8;--accent2:#0ea5e9;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:var(--sans);background:var(--bg);color:var(--text);min-height:100vh;}

  /* ── header ── */
  header{
    background:var(--bg-header);color:var(--text-header);
    padding:0 32px;height:56px;display:flex;align-items:center;
    justify-content:space-between;position:sticky;top:0;z-index:100;
    box-shadow:0 2px 16px rgba(0,0,0,0.5);
  }
  .hdr-left{display:flex;align-items:center;gap:14px;}
  .logo{width:52px;height:52px;border-radius:6px;overflow:hidden;
    display:flex;align-items:center;justify-content:center;flex-shrink:0;}
  .logo img{width:52px;height:52px;object-fit:contain;}
  .logo.fallback{border-radius:50%;
    background:conic-gradient(from 0deg,#00c8ff 0%,#0047ab 45%,#00c8ff 100%);
    font-size:12px;font-weight:700;color:#fff;box-shadow:0 0 12px rgba(0,200,255,0.35);}
  .hdr-title{font-size:15px;font-weight:600;letter-spacing:.01em;}
  .hdr-sub{font-size:11px;opacity:.55;margin-top:1px;}
  .badge{display:inline-flex;align-items:center;gap:5px;
    font-size:11px;padding:3px 10px;border-radius:20px;font-weight:600;}
  .badge-blue{background:rgba(56,189,248,.12);color:#38bdf8;border:1px solid rgba(56,189,248,.25);}
  .badge-green{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25);}
  .badge-red{background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.25);}
  .badge-gold{background:rgba(251,191,36,.1);color:#fbbf24;border:1px solid rgba(251,191,36,.2);}
  .theme-btn{background:transparent;border:1px solid rgba(255,255,255,.25);color:var(--text-header);
    border-radius:20px;padding:4px 12px;font-size:11px;cursor:pointer;font-family:var(--sans);}
  .theme-btn:hover{background:rgba(255,255,255,.08);}

  /* ── layout ── */
  .page{max-width:1100px;margin:0 auto;padding:28px 24px 48px;}

  /* ── cards ── */
  .card{background:var(--bg-card);border:1px solid var(--border);border-radius:10px;
    padding:20px 24px;margin-bottom:20px;box-shadow:0 1px 4px rgba(15,23,42,.08);}
  .card-header{display:flex;align-items:center;justify-content:space-between;
    padding-bottom:14px;margin-bottom:16px;border-bottom:1px solid var(--border);}
  .card-title{font-size:11px;font-weight:700;letter-spacing:.1em;
    text-transform:uppercase;color:var(--text-muted);display:flex;align-items:center;gap:8px;}
  .dot{width:6px;height:6px;border-radius:50%;background:#38bdf8;
    box-shadow:0 0 8px #38bdf8;flex-shrink:0;}
  .dot-green{background:#22c55e;box-shadow:0 0 8px #22c55e;}
  .dot-red{background:#ef4444;box-shadow:0 0 8px #ef4444;}
  .dot-gold{background:#fbbf24;box-shadow:0 0 8px #fbbf24;}

  /* ── form elements ── */
  label.field-label{display:block;font-size:11px;font-weight:700;
    letter-spacing:.07em;text-transform:uppercase;color:var(--text-muted);margin-bottom:5px;}
  .field{margin-bottom:16px;}
  input[type=text],input[type=number]{
    padding:7px 11px;border:1px solid var(--border);border-radius:6px;
    font-size:13px;font-family:var(--sans);background:var(--bg-card);color:var(--text);
    outline:none;transition:border-color .15s,box-shadow .15s;width:100%;}
  input[type=text]:focus,input[type=number]:focus{
    border-color:var(--accent);box-shadow:0 0 0 3px rgba(2,132,199,.12);}
  input[type=number]{width:110px;}
  input[type=checkbox],input[type=radio]{accent-color:var(--accent);}
  .row{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;}
  .row .field{margin-bottom:0;}

  /* ── step cards ── */
  .steps-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px;}
  .step-card{border:1px solid var(--border);border-radius:8px;padding:10px 14px;
    display:flex;align-items:flex-start;gap:10px;cursor:pointer;
    transition:border-color .15s,background .15s;}
  .step-card:hover{border-color:var(--accent);background:rgba(2,132,199,.04);}
  .step-card input[type=checkbox]{margin-top:3px;flex-shrink:0;}
  .step-name{font-size:13px;font-weight:600;margin-bottom:2px;}
  .step-cmd{font-family:var(--mono);font-size:11px;color:var(--accent);margin-bottom:3px;}
  .step-desc{font-size:11px;color:var(--text-muted);}

  /* ── step presets ── */
  .preset-bar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;}
  .preset-btn{padding:6px 16px;border-radius:20px;border:1px solid var(--border);
    font-size:12px;font-weight:600;cursor:pointer;background:var(--bg-card);color:var(--text-muted);
    font-family:var(--sans);transition:all .15s;white-space:nowrap;}
  .preset-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(2,132,199,.06);}
  .preset-btn.active{background:rgba(56,189,248,.12);color:#38bdf8;border-color:rgba(56,189,248,.4);}

  /* ── temperature mode toggle ── */
  .temp-modes{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;}
  .mode-btn{padding:5px 14px;border-radius:20px;border:1px solid var(--border);
    font-size:12px;font-weight:600;cursor:pointer;background:var(--bg-card);color:var(--text-muted);
    font-family:var(--sans);transition:all .15s;}
  .mode-btn.active{background:var(--accent);color:#fff;border-color:var(--accent);}

  /* ── buttons ── */
  .btn{display:inline-flex;align-items:center;gap:7px;padding:9px 20px;
    border-radius:6px;font-size:13px;font-weight:600;font-family:var(--sans);
    cursor:pointer;border:none;transition:opacity .15s,box-shadow .15s;letter-spacing:.02em;}
  .btn:hover{opacity:.88;}
  .btn-primary{background:linear-gradient(135deg,#0369a1,#0ea5e9);color:#fff;
    box-shadow:0 2px 12px rgba(14,165,233,.25);}
  .btn-submit{background:linear-gradient(135deg,#15803d,#22c55e);color:#fff;
    box-shadow:0 2px 12px rgba(34,197,94,.25);}
  .btn-clear{background:rgba(148,163,184,.12);color:var(--text-muted);
    border:1px solid var(--border);}

  /* ── script preview ── */
  .script-pre{background:#060e1c;color:#cbd5e1;padding:16px 18px;border-radius:8px;
    font-family:var(--mono);font-size:12px;line-height:1.7;overflow-x:auto;
    white-space:pre;border:1px solid rgba(56,189,248,.12);max-height:420px;overflow-y:auto;}
  .qsub-bar{padding:10px 14px;background:rgba(56,189,248,.05);
    border:1px solid rgba(56,189,248,.18);border-radius:6px;
    font-family:var(--mono);font-size:12px;margin-top:12px;
    display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
  .qsub-label{font-size:11px;color:var(--text-muted);flex-shrink:0;}

  /* ── job history table ── */
  table{border-collapse:collapse;width:100%;font-size:12px;}
  th{text-align:left;font-size:10px;font-weight:700;letter-spacing:.08em;
    text-transform:uppercase;color:var(--text-muted);padding:7px 12px;
    background:rgba(148,163,184,.08);border-bottom:1px solid var(--border);}
  td{padding:8px 12px;border-bottom:1px solid var(--border);
    font-family:var(--mono);vertical-align:top;}
  tr:last-child td{border-bottom:none;}
  tr:hover td{background:rgba(148,163,184,.04);}
  .job-ok{color:#22c55e;font-weight:600;}
  .job-fail{color:#ef4444;font-weight:600;}

  /* ── alerts ── */
  .alert-error{background:rgba(239,68,68,.06);border:1px solid rgba(239,68,68,.25);
    color:#dc2626;border-radius:6px;padding:10px 14px;font-size:13px;margin-bottom:12px;}
  .alert-success{background:rgba(34,197,94,.06);border:1px solid rgba(34,197,94,.25);
    color:#15803d;border-radius:6px;padding:12px 16px;font-size:14px;font-weight:600;}
  code{font-family:var(--mono);font-size:11px;background:rgba(148,163,184,.12);
    padding:1px 5px;border-radius:3px;}

  /* ── path preview ── */
  .path-preview{display:none;margin-top:8px;padding:8px 12px;
    background:rgba(2,132,199,.06);border:1px solid rgba(2,132,199,.2);
    border-radius:6px;font-size:12px;line-height:1.6;}
  .path-preview .pp-label{color:var(--text-muted);font-size:11px;font-weight:600;
    letter-spacing:.06em;text-transform:uppercase;margin-bottom:3px;}
  .path-preview .pp-base{font-family:var(--mono);color:var(--text);}
  .path-preview .pp-temp{font-family:var(--mono);color:#f59e0b;font-weight:700;}
  .path-preview .pp-full{font-family:var(--mono);font-size:11px;color:var(--text-muted);margin-top:4px;}

  /* ── watcher ── */
  .watch-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;}
  @media(max-width:700px){.watch-grid{grid-template-columns:1fr;}}
  .watch-pane{border:1px solid var(--border);border-radius:8px;overflow:hidden;}
  .watch-pane-title{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
    color:var(--text-muted);padding:8px 14px;background:rgba(148,163,184,.06);
    border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;}
  .watch-body{padding:10px 12px;max-height:340px;overflow-y:auto;}
  .temp-row{display:flex;align-items:center;gap:10px;padding:5px 0;
    border-bottom:1px solid var(--border);font-size:12px;}
  .temp-row:last-child{border-bottom:none;}
  .temp-num{font-family:var(--mono);font-weight:700;min-width:44px;color:#fbbf24;}
  .prog-bar-wrap{flex:1;height:8px;background:rgba(148,163,184,.15);
    border-radius:4px;overflow:hidden;}
  .prog-bar{height:100%;border-radius:4px;transition:width .4s ease;}
  .prog-done{background:linear-gradient(90deg,#16a34a,#22c55e);}
  .prog-mid{background:linear-gradient(90deg,#0369a1,#38bdf8);}
  .prog-raw{background:rgba(148,163,184,.35);}
  .prog-empty{background:rgba(148,163,184,.1);}
  .temp-count{font-family:var(--mono);font-size:11px;min-width:32px;
    text-align:right;color:var(--text-muted);}
  .temp-status{font-size:10px;font-weight:700;letter-spacing:.05em;min-width:68px;text-align:right;}
  .st-done{color:#22c55e;}
  .st-proc{color:#38bdf8;}
  .st-raw{color:#fbbf24;}
  .st-empty{color:var(--text-muted);}
  /* Queue is a terminal-style panel: always a dark background with bright
     text so the job-ID/name/user columns and the colour-coded statuses stay
     readable in BOTH the light and dark themes. */
  .qstat-pre{font-family:var(--mono);font-size:11.5px;line-height:1.6;
    white-space:pre;overflow-x:auto;color:#f1f5f9;
    background:#0b1627;border:1px solid #27476e;border-radius:7px;
    padding:9px 11px;}
  .watch-bar{display:flex;align-items:center;gap:10px;padding:8px 14px;
    background:rgba(148,163,184,.04);border-top:1px solid var(--border);
    font-size:11px;color:var(--text-muted);}
  .pulse{width:7px;height:7px;border-radius:50%;background:#22c55e;
    box-shadow:0 0 6px #22c55e;animation:pulse 1.5s infinite;}
  .pulse.paused{background:#64748b;box-shadow:none;animation:none;}
  @keyframes pulse{0%,100%{opacity:1;}50%{opacity:.35;}}
  .watch-input-row{display:flex;gap:8px;align-items:center;padding:10px 14px;
    border-bottom:1px solid var(--border);}

  /* ── auto-scan panel ── */
  .scan-panel{margin-top:10px;border:1px solid var(--border);border-radius:8px;overflow:hidden;}
  .scan-row{display:flex;align-items:flex-start;gap:12px;padding:10px 14px;
    border-bottom:1px solid var(--border);font-size:13px;}
  .scan-row:last-child{border-bottom:none;}
  .scan-label{font-size:11px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;
    color:var(--text-muted);min-width:110px;padding-top:2px;flex-shrink:0;}
  .scan-chips{display:flex;flex-wrap:wrap;gap:6px;}
  .chip{font-family:var(--mono);font-size:12px;padding:2px 9px;border-radius:12px;font-weight:600;}
  .chip-green{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25);}
  .chip-muted{background:rgba(148,163,184,.1);color:var(--text-muted);border:1px solid var(--border);}
  .chip-gold{background:rgba(251,191,36,.1);color:#fbbf24;border:1px solid rgba(251,191,36,.2);}
  /* selectable temp chips */
  .temp-check{display:inline-flex;align-items:center;gap:5px;font-family:var(--mono);
    font-size:12px;padding:3px 9px 3px 7px;border-radius:12px;font-weight:600;cursor:pointer;
    background:rgba(251,191,36,.08);color:#fbbf24;border:1px solid rgba(251,191,36,.25);
    transition:opacity .12s,background .12s;user-select:none;}
  .temp-check input{accent-color:#d97706;margin:0;cursor:pointer;}
  .temp-check.unchecked{opacity:.4;background:rgba(148,163,184,.08);color:var(--text-muted);
    border-color:var(--border);}
  .sel-btn{padding:3px 10px;border-radius:14px;border:1px solid var(--border);
    font-size:11px;font-weight:600;cursor:pointer;background:var(--bg-card);
    color:var(--text-muted);font-family:var(--sans);}
  .sel-btn:hover{border-color:var(--accent);color:var(--accent);}
  .scan-status{font-size:12px;color:var(--text-muted);padding:14px;text-align:center;}
  .scan-error{color:#ef4444;font-size:13px;padding:12px 14px;}

  /* ── misc ── */
  .hint{font-size:11px;color:var(--text-muted);margin-top:4px;}
  .divider{height:1px;background:var(--border);margin:20px 0;}
  ::-webkit-scrollbar{width:5px;height:5px;}
  ::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}

  /* ── directory picker modal ── */
  .modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:500;
    display:none;align-items:center;justify-content:center;}
  .modal-overlay.open{display:flex;}
  .modal-box{background:var(--bg-card);border:1px solid var(--border);border-radius:12px;
    width:min(680px,95vw);max-height:80vh;display:flex;flex-direction:column;
    box-shadow:0 20px 60px rgba(0,0,0,.45);}
  .modal-header{padding:16px 20px;border-bottom:1px solid var(--border);
    display:flex;align-items:center;justify-content:space-between;flex-shrink:0;}
  .modal-title{font-size:13px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-muted);}
  .modal-close{background:none;border:none;font-size:20px;cursor:pointer;color:var(--text-muted);padding:0 4px;line-height:1;}
  .modal-close:hover{color:var(--text);}
  .modal-crumbs{padding:10px 20px;border-bottom:1px solid var(--border);
    display:flex;flex-wrap:wrap;gap:4px;align-items:center;flex-shrink:0;font-size:12px;}
  .crumb{color:var(--accent);cursor:pointer;font-family:var(--mono);}
  .crumb:hover{text-decoration:underline;}
  .crumb-sep{color:var(--text-muted);margin:0 2px;}
  .modal-body{flex:1;overflow-y:auto;padding:10px 12px;}
  .dir-item{display:flex;align-items:center;gap:10px;padding:7px 12px;
    border-radius:6px;cursor:pointer;font-size:13px;font-family:var(--mono);}
  .dir-item:hover{background:rgba(2,132,199,.08);color:var(--accent);}
  .modal-footer{padding:12px 20px;border-top:1px solid var(--border);
    display:flex;gap:10px;align-items:center;flex-shrink:0;}
  .modal-cur{font-family:var(--mono);font-size:11px;color:var(--text-muted);
    flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .browse-btn{padding:6px 14px;border-radius:6px;border:1px solid var(--border);
    background:rgba(2,132,199,.08);color:var(--accent);font-size:12px;font-weight:600;
    font-family:var(--sans);cursor:pointer;white-space:nowrap;}
  .browse-btn:hover{background:rgba(2,132,199,.2);}

  /* ── tab bar (modern shell) ── */
  .tabbar{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:22px;
    background:var(--bg-card);border:1px solid var(--border);border-radius:12px;
    padding:6px;box-shadow:0 2px 10px rgba(15,23,42,.08);position:sticky;top:64px;z-index:60;}
  .tab-btn{flex:1 1 150px;display:flex;align-items:center;justify-content:center;gap:8px;
    padding:11px 14px;border-radius:9px;border:1px solid transparent;background:transparent;
    color:var(--text-muted);font-size:13px;font-weight:600;font-family:var(--sans);cursor:pointer;
    letter-spacing:.01em;transition:all .15s;white-space:nowrap;}
  .tab-btn:hover{background:rgba(148,163,184,.12);color:var(--text);}
  .tab-btn .tab-ico{font-size:15px;line-height:1;}
  .tab-btn.active{background:linear-gradient(135deg,#0369a1,#0ea5e9);color:#fff;
    box-shadow:0 3px 14px rgba(14,165,233,.35);}
  .tab-btn.active.tab-general{background:linear-gradient(135deg,#334155,#64748b);box-shadow:0 3px 14px rgba(100,116,139,.35);}
  .tab-btn.active.tab-submit{background:linear-gradient(135deg,#0369a1,#0ea5e9);box-shadow:0 3px 14px rgba(14,165,233,.35);}
  .tab-btn.active.tab-scan{background:linear-gradient(135deg,#b45309,#f59e0b);box-shadow:0 3px 14px rgba(245,158,11,.35);}
  .tab-btn.active.tab-watch{background:linear-gradient(135deg,#047857,#10b981);box-shadow:0 3px 14px rgba(16,185,129,.35);}
  .tab-btn.active.tab-auto{background:linear-gradient(135deg,#6d28d9,#a855f7);box-shadow:0 3px 14px rgba(168,85,247,.35);}
  .tab-btn.active.tab-health{background:linear-gradient(135deg,#b91c1c,#ef4444);box-shadow:0 3px 14px rgba(239,68,68,.35);}
  .tabpanel{display:none;animation:tabfade .2s ease;}
  .tabpanel.active{display:block;}
  @keyframes tabfade{from{opacity:0;transform:translateY(5px);}to{opacity:1;transform:none;}}
  /* ── brighter Live Watcher ── */
  #watcher-card{border-color:rgba(16,185,129,.45);box-shadow:0 2px 14px rgba(16,185,129,.12);}
  #watcher-card .card-title{color:#10b981;}
  #watcher-card .watch-pane{border-color:rgba(16,185,129,.3);}
  #watcher-card .watch-pane-title{background:linear-gradient(90deg,rgba(16,185,129,.14),rgba(56,189,248,.08));color:var(--text);}
  #watcher-card .watch-bar{background:rgba(16,185,129,.06);color:var(--text);}
</style>
<script>
function applyTheme(){if(localStorage.getItem('nxjobs_theme')==='dark')document.body.classList.add('dark');}
function toggleTheme(){document.body.classList.toggle('dark');localStorage.setItem('nxjobs_theme',document.body.classList.contains('dark')?'dark':'light');}

function setTempMode(mode){
  if(['single','range','list'].indexOf(mode)<0) mode='single';
  document.querySelectorAll('.mode-btn').forEach(function(b){b.classList.remove('active');});
  var mb=document.getElementById('modeBtn_'+mode); if(mb) mb.classList.add('active');
  document.getElementById('temp_mode_val').value=mode;
  ['single','range','list'].forEach(function(m){
    var el=document.getElementById('tblock_'+m); if(el) el.style.display=(m===mode)?'':'none';
  });
  updatePathPreview();
}

// ── Live path + script-name preview ───────────────────────────────────────────
function _escHtml(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function _getFirstTemp(){
  var mode=document.getElementById('temp_mode_val').value;
  if(mode==='single'){
    return (document.getElementById('temp_single_input')||{value:''}).value.trim();
  } else if(mode==='range'){
    return (document.getElementById('temp_start_input')||{value:''}).value.trim();
  } else {
    var raw=((document.getElementById('temp_list_input')||{value:''}).value||'');
    return raw.replace(/,/g,' ').trim().split(/\s+/)[0]||'';
  }
}

// Mirror of Python _script_name — runs client-side so user sees the name instantly
function _piShort(piFolder){
  // "wilson-4797-a" → "wilson"  (stop before first all-digit segment)
  var parts=piFolder.split('-'), result=[];
  for(var i=0;i<parts.length;i++){
    if(/^\d+$/.test(parts[i])) break;
    result.push(parts[i]);
  }
  return result.length ? result.join('-') : (parts[0]||'job');
}
function _getScriptName(){
  var root='{{ root_path }}';
  var ud=((document.getElementById('user_dir_input')||{value:''}).value||'').trim().replace(/\/+$/,'');
  if(!ud) return '';
  // Strip trailing numeric component (temperature dir) if present
  var parts=ud.split('/').filter(Boolean);
  if(parts.length && /^\d+$/.test(parts[parts.length-1])) parts.pop();
  ud='/'+parts.join('/');
  var rootSlash=root.endsWith('/')?root:root+'/';
  var rel=ud.startsWith(rootSlash)?ud.slice(rootSlash.length):ud.replace(/^\//,'');
  var piFolder=rel.split('/').filter(Boolean)[0]||'job';
  var piShort=_piShort(piFolder);
  var sample=(parts[parts.length-1])||'sample';
  var temp=_getFirstTemp();
  if(!temp) return '';
  var base=(piShort+'-'+sample+'-'+temp).replace(/[^\w\-]/g,'_');
  return base+'.sh';
}

function updatePathPreview(){
  var el=document.getElementById('path-preview');
  if(!el) return;
  var ud=(document.getElementById('user_dir_input')||{value:''}).value.trim();
  var temp=_getFirstTemp();
  if(!ud){el.style.display='none'; return;}
  var base=ud.endsWith('/')?ud:ud+'/';
  var sname=_getScriptName();
  el.style.display='';
  var html='<div class="pp-label">Path &amp; script name preview</div>';
  if(temp){
    html+='<div><span class="pp-base">'+_escHtml(base)+'</span>'
         +'<span class="pp-temp">'+_escHtml(temp)+'/</span></div>';
  } else {
    html+='<div><span class="pp-base">'+_escHtml(base)+'</span>'
         +'<span style="color:var(--text-muted);font-family:var(--mono);">&lt;TEMP&gt;/</span></div>';
  }
  if(sname){
    html+='<div style="margin-top:5px;">'
         +'<span style="color:var(--text-muted);font-size:11px;">Script name: </span>'
         +'<span style="font-family:var(--mono);color:#a78bfa;font-weight:600;">'+_escHtml(sname)+'</span>'
         +'</div>';
  }
  el.innerHTML=html;
}

function copyScript(){
  var el=document.getElementById('script-pre-content');
  if(!el) return;
  navigator.clipboard.writeText(el.innerText).then(function(){
    var btn=document.getElementById('copy-btn');
    btn.textContent='Copied!';
    setTimeout(function(){btn.textContent='Copy Script';},1500);
  });
}

// ── Directory picker ──────────────────────────────────────────────────────────
var _dpTarget = null;   // which input field to populate

function openDirPicker(targetId){
  _dpTarget = targetId;
  document.getElementById('dir-modal').classList.add('open');
  _dpNav('{{ root_path }}');
}
function closeDirPicker(){
  document.getElementById('dir-modal').classList.remove('open');
}
function _dpNav(path){
  document.getElementById('dp-loading').style.display='';
  document.getElementById('dp-list').innerHTML='';
  fetch('/browse_dir?path='+encodeURIComponent(path))
    .then(function(r){return r.json();})
    .then(function(d){
      document.getElementById('dp-loading').style.display='none';
      // breadcrumbs
      var crumbs=document.getElementById('dp-crumbs');
      crumbs.innerHTML='';
      var parts=d.path.split('/').filter(Boolean);
      var built='';
      parts.forEach(function(p,i){
        built+='/'+p;
        var sp=document.createElement('span');
        sp.className='crumb'; sp.textContent=p;
        (function(cp){sp.onclick=function(){_dpNav(cp);};})(built);
        crumbs.appendChild(sp);
        if(i<parts.length-1){var s=document.createElement('span');s.className='crumb-sep';s.textContent='/';crumbs.appendChild(s);}
      });
      // current path footer
      document.getElementById('dp-cur').textContent=d.path;
      // directory list
      var ul=document.getElementById('dp-list');
      ul.innerHTML='';
      // Add ".." to go up, unless already at /nfs/chess
      var parentPath=d.path.split('/').slice(0,-1).join('/');
      if(parentPath && parentPath.length > '/nfs/chess'.length){
        var upRow=document.createElement('div');
        upRow.className='dir-item';
        upRow.innerHTML='<span style="font-size:16px;">&#8593;</span><span style="color:var(--text-muted);">.. (up one level)</span>';
        upRow.onclick=function(){_dpNav(parentPath);};
        ul.appendChild(upRow);
      }
      if(d.dirs.length===0){
        var empty=document.createElement('div');
        empty.style.cssText='padding:20px;color:var(--text-muted);font-size:12px;text-align:center;';
        empty.textContent='No sub-folders';
        ul.appendChild(empty);
      } else {
        d.dirs.forEach(function(dir){
          var row=document.createElement('div');
          row.className='dir-item';
          row.innerHTML='<span style="font-size:16px;">&#128193;</span><span>'+dir.name+'</span>';
          row.onclick=function(){_dpNav(dir.path);};
          ul.appendChild(row);
        });
      }
    })
    .catch(function(e){
      document.getElementById('dp-loading').style.display='none';
      document.getElementById('dp-list').innerHTML='<div style="padding:20px;color:#ef4444;font-size:12px;">Error: '+e+'</div>';
    });
}
function _dpSelect(){
  var p=document.getElementById('dp-cur').textContent;
  var selected=p.endsWith('/')?p:p+'/';
  if(_dpTarget==='user_dir_input'){
    // If last path component is purely numeric (e.g. /91/), it's a temperature dir —
    // strip it from USER_DIR and auto-fill the temperature input instead.
    var trimmed=selected.replace(/\/+$/,'');
    var lastComp=trimmed.split('/').pop();
    if(/^\d+$/.test(lastComp)){
      var parentPath=trimmed.split('/').slice(0,-1).join('/')+'/';
      document.getElementById('user_dir_input').value=parentPath;
      var tempEl=document.getElementById('temp_single_input');
      if(tempEl){ tempEl.value=lastComp; }
      setTempMode('single');
    } else {
      document.getElementById('user_dir_input').value=selected;
    }
    // Auto-fill chmod path as first-level folder below ROOT
    var useDir=document.getElementById('user_dir_input').value;
    var root='{{ root_path }}';
    var rootSlash=root.endsWith('/')?root:root+'/';
    if(useDir.startsWith(rootSlash)){
      var rel=useDir.slice(rootSlash.length);
      var piFolder=rel.split('/')[0];
      if(piFolder){
        var chmodEl=document.getElementById('chmod_path_input');
        if(chmodEl && !chmodEl.value.trim())
          chmodEl.value=rootSlash+piFolder+'/';
      }
    }
    updatePathPreview();
    if(typeof watchMirrorFromUserDir==='function') watchMirrorFromUserDir();
  } else {
    var targetEl=document.getElementById(_dpTarget);
    if(targetEl) targetEl.value=selected;
    if(_dpTarget==='watch-dir-input'){
      watchDirChanged();
    }
  }
  closeDirPicker();
}

// ── Step presets ──────────────────────────────────────────────────────────────
var _PRESETS = {
  'parent_avail': {
    label: 'Parent file (available)',
    steps: {load:1, link_copy:1, link_only:0, link_ow:0, copy_only:0, find:1, refine:1, transform:1, pdf:0}
  },
  'parent_not_avail': {
    label: 'Parent file (not available)',
    steps: {load:1, link_copy:0, link_only:1, link_ow:0, copy_only:0, find:1, refine:0, transform:0, pdf:0}
  },
  'after_parent': {
    label: 'After parent file available',
    steps: {load:0, link_copy:0, link_only:0, link_ow:0, copy_only:1, find:0, refine:1, transform:1, pdf:0}
  }
};
var _activePreset = null;

function applyPreset(key){
  var preset = _PRESETS[key];
  if(!preset) return;
  // Toggle off if already active
  if(_activePreset === key){
    _activePreset = null;
    document.querySelectorAll('.preset-btn').forEach(function(b){b.classList.remove('active');});
    return;
  }
  _activePreset = key;
  // Update preset button highlight
  document.querySelectorAll('.preset-btn').forEach(function(b){
    b.classList.toggle('active', b.dataset.preset===key);
  });
  // Set each step checkbox
  Object.keys(preset.steps).forEach(function(s){
    var el = document.getElementById('step_'+s);
    if(el) el.checked = (preset.steps[s] === 1);
  });
}

// ── Auto-scan mode ────────────────────────────────────────────────────────────
var _scanTemps = [];   // temps confirmed for processing

function scanModeChanged(){
  var exact = _scanModeExact();
  var hidden = document.getElementById('autoscan_exact_val');
  if(hidden) hidden.value = exact ? '1' : '0';
  var desc = document.getElementById('scan-mode-desc');
  if(desc){
    desc.innerHTML = exact
      ? '<strong>Exact:</strong> opens each temperature\u2019s .nxs, detects completed stages (load \u2192 link/copy/max \u2192 find \u2192 refine \u2192 transform), and the script resumes from the first incomplete stage.'
      : '<strong>File-count:</strong> folders with 10+ files are treated as complete; exactly 3 files skips Load; otherwise full chain.';
  }
}
function _scanModeExact(){
  var r = document.querySelector('input[name="scan_mode_radio"]:checked');
  return r && r.value === 'exact';
}

function triggerScan(){
  var sEl = document.getElementById('scan_user_dir');
  var fEl = document.getElementById('user_dir_input');
  var ud  = ((sEl&&sEl.value.trim()) || (fEl&&fEl.value.trim()) || '');
  if(!ud){ _renderScanPanel({error:'Enter a Sample Directory (USER_DIR) first.'}); return; }
  if(fEl) fEl.value = ud;
  var exact = _scanModeExact();
  var eEl = document.getElementById('scan_entries');
  var ent = ((eEl&&eEl.value.trim()) || (document.getElementById('entries_input')||{value:'f1 f2 f3'}).value.trim() || 'f1 f2 f3');
  var fe = document.getElementById('entries_input'); if(fe) fe.value = ent;
  var hidden = document.getElementById('autoscan_exact_val'); if(hidden) hidden.value = exact ? '1' : '0';
  _renderScanPanel({status:'Scanning ' + ud + (exact ? ' (reading .nxs files\u2026)' : ' \u2026')});
  var url = '/scan_temps?user_dir=' + encodeURIComponent(ud)
          + '&exact=' + (exact ? '1' : '0')
          + '&entries=' + encodeURIComponent(ent);
  fetch(url)
    .then(function(r){return r.json();})
    .then(function(d){ _renderScanPanel(d); })
    .catch(function(e){ _renderScanPanel({error:'Scan failed: ' + e}); });
}

function _renderScanPanel(d){
  var el = document.getElementById('autoscan-panel');
  if(!el) return;
  _scanTemps = [];
  if(d.status){
    el.innerHTML = '<div class="scan-status">⏳ ' + _escHtml(d.status) + '</div>';
    return;
  }
  if(d.error){
    el.innerHTML = '<div class="scan-error">⚠ ' + _escHtml(d.error) + '</div>';
    return;
  }
  _scanTemps = d.to_process || [];
  var html = '';
  // Parent file row — green if present, gold "no parent" mode otherwise
  if(d.has_parent){
    html += '<div class="scan-row">'
          + '<span class="scan-label">Parent file</span>'
          + '<span class="chip chip-green">✓ ' + _escHtml(d.parent_file) + '</span>'
          + '</div>';
  } else {
    html += '<div class="scan-row">'
          + '<span class="scan-label" style="color:#fbbf24;">No parent</span>'
          + '<span style="font-size:12px;color:#fbbf24;">No <code>*_parent.nxs</code> found — '
          + 'generating <strong>load, link, max, find</strong> only (no copy/refine/transform).</span>'
          + '</div>';
  }

  // h5py warning (exact mode without h5py falls back to full chain)
  if(d.exact && d.h5_note === 'h5py-unavailable'){
    html += '<div class="scan-row"><span class="scan-label" style="color:#f59e0b;">Note</span>'
          + '<span style="font-size:12px;color:#f59e0b;">h5py not available on server — stage detection skipped, full chain will be generated. '
          + 'Install h5py to enable exact resume.</span></div>';
  }

  // Build a lookup of temp -> detail (exact mode)
  var detailMap = {};
  if(d.exact && d.details){
    d.details.forEach(function(x){ detailMap[x.temp] = x; });
  }
  var stageOrder = d.stage_order || ['load','link','copy','max','find','refine','transform'];
  var stageShort = {load:'load',link:'link',copy:'copy',max:'max',find:'find',refine:'refine',transform:'transform',combine:'combine'};

  // To process — selectable checkboxes (all pre-checked)
  if(_scanTemps.length){
    html += '<div class="scan-row"><span class="scan-label">Will process<br>'
          + '<span style="font-weight:400;color:#38bdf8;" id="scan-count-label">('
          + _scanTemps.length + ' jobs)</span><br>'
          + '<span style="display:flex;gap:5px;margin-top:6px;">'
          + '<button type="button" class="sel-btn" onclick="scanSelectAll(true)">All</button>'
          + '<button type="button" class="sel-btn" onclick="scanSelectAll(false)">None</button>'
          + '</span></span>'
          + '<div class="scan-chips" id="scan-chip-box" style="flex-direction:'
          + (d.exact ? 'column' : 'row') + ';align-items:'
          + (d.exact ? 'stretch' : 'flex-start') + ';">';
    _scanTemps.forEach(function(t){
      if(d.exact && detailMap[t]){
        var det = detailMap[t];
        var rem = det.remaining || [];
        var remSet = {}; rem.forEach(function(s){ remSet[s]=1; });
        // stage pills: green=done, gold=to-run
        var pills = stageOrder.map(function(s){
          var todo = remSet[s];
          var col = todo ? 'background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);'
                         : 'background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25);';
          var mark = todo ? '○' : '✓';
          return '<span style="font-size:10px;padding:1px 6px;border-radius:8px;'+col+'">'+mark+' '+stageShort[s]+'</span>';
        }).join(' ');
        var nxsLabel = det.nxs ? det.nxs : '(no .nxs found)';
        html += '<label class="temp-check" data-temp="'+_escHtml(t)+'" '
              + 'style="display:flex;align-items:center;gap:8px;justify-content:flex-start;padding:6px 10px;">'
              + '<input type="checkbox" checked onchange="scanSyncSelected()">'
              + '<span style="min-width:46px;font-weight:700;">'+_escHtml(t)+'K</span>'
              + '<span style="font-size:10px;color:var(--text-muted);min-width:120px;">'+_escHtml(nxsLabel)+'</span>'
              + '<span style="display:flex;gap:4px;flex-wrap:wrap;">'+pills+'</span>'
              + '</label>';
      } else {
        html += '<label class="temp-check" data-temp="'+_escHtml(t)+'">'
              + '<input type="checkbox" checked onchange="scanSyncSelected()">'
              + _escHtml(t)+'K</label>';
      }
    });
    html += '</div></div>';
  } else {
    html += '<div class="scan-row"><span class="scan-label">Will process</span>'
          + '<span style="font-size:13px;color:var(--text-muted);">None — everything is already complete.</span>'
          + '</div>';
  }
  // Already done
  if((d.already_done||[]).length){
    html += '<div class="scan-row"><span class="scan-label">Skip (done)<br>'
          + '<span style="font-weight:400;font-size:10px;">'+(d.exact?'all stages complete':'10+ files')+'</span></span>'
          + '<div class="scan-chips">';
    d.already_done.forEach(function(t){
      html += '<span class="chip chip-muted">'+_escHtml(t)+'</span>';
    });
    html += '</div></div>';
  }
  // Skip — raw scans missing / insufficient
  if((d.raw_skipped||[]).length){
    html += '<div class="scan-row"><span class="scan-label" style="color:#fbbf24;">Skip (done)<br>'
          + '<span style="font-weight:400;font-size:10px;">less raw files</span></span>'
          + '<div style="display:flex;flex-direction:column;gap:4px;flex:1;">';
    d.raw_skipped.forEach(function(r){
      html += '<div style="display:flex;align-items:center;gap:8px;font-size:11px;">'
            + '<span class="chip chip-muted" style="font-size:11px;">'+_escHtml(r.temp)+'K</span>'
            + '<span style="color:#f59e0b;">'+_escHtml(r.reason||'raw scans missing')+'</span>'
            + '</div>';
    });
    html += '</div></div>';
  }
  // (generate controls live in the Exact .nxs Scan tab, not inline)
  el.innerHTML = html;
  scanSyncSelected();
}

// Main bottom buttons — set action; in autoscan mode require a selection
function mainGenerate(action){
  document.getElementById('form-action').value = action;
  var mode = document.getElementById('temp_mode_val').value;
  if(mode === 'autoscan'){
    scanSyncSelected();
    var sel = (document.getElementById('autoscan_selected_val')||{value:''}).value.trim();
    if(!sel){
      alert('Auto-Scan mode: no temperatures selected. Click "Scan Now", tick the temperatures you want, then Generate.');
      return false;
    }
  }
  return true;
}

// Submit the form in autoscan mode for the ticked temps only
function autoscanGenerate(action){
  scanSyncSelected();
  var sel = (document.getElementById('autoscan_selected_val')||{value:''}).value.trim();
  if(!sel){
    alert('No temperatures selected. Tick at least one temperature first.');
    return;
  }
  // Force the mode + action, then submit the main form
  document.getElementById('temp_mode_val').value = 'autoscan';
  document.getElementById('form-action').value = action;
  // Find and submit the form
  var f = document.getElementById('temp_mode_val').form
          || document.querySelector('form[method="POST"]');
  if(f) f.submit();
}

// Keep the hidden form field + count label in sync with ticked checkboxes
function scanSyncSelected(){
  var box = document.getElementById('scan-chip-box');
  var hidden = document.getElementById('autoscan_selected_val');
  if(!box){ if(hidden) hidden.value=''; return; }
  var picked = [];
  box.querySelectorAll('.temp-check').forEach(function(lbl){
    var cb = lbl.querySelector('input[type=checkbox]');
    if(cb && cb.checked){
      picked.push(lbl.getAttribute('data-temp'));
      lbl.classList.remove('unchecked');
    } else {
      lbl.classList.add('unchecked');
    }
  });
  if(hidden) hidden.value = picked.join(',');
  var lbl = document.getElementById('scan-count-label');
  if(lbl) lbl.textContent = '(' + picked.length + ' job' + (picked.length!==1?'s':'') + ')';
}

function scanSelectAll(state){
  var box = document.getElementById('scan-chip-box');
  if(!box) return;
  box.querySelectorAll('.temp-check input[type=checkbox]').forEach(function(cb){
    cb.checked = state;
  });
  scanSyncSelected();
}

document.addEventListener('DOMContentLoaded',function(){
  applyTheme();
  var m=document.getElementById('temp_mode_val');
  if(m){ var _mv=m.value||'single'; if(['single','range','list'].indexOf(_mv)<0)_mv='single'; setTempMode(_mv); }
  // close modal on overlay click
  document.getElementById('dir-modal').addEventListener('click',function(e){
    if(e.target===this) closeDirPicker();
  });
  // live path preview on typing
  ['user_dir_input','temp_single_input','temp_start_input','temp_list_input'].forEach(function(id){
    var el=document.getElementById(id);
    if(el) el.addEventListener('input', updatePathPreview);
  });
  updatePathPreview();
});
// ── Live Watcher ──────────────────────────────────────────────────────────────
var _watchTimer   = null;
var _watchPaused  = true;
var _watchDir     = '';
var _watchInterval = 5000;

// Max files we expect in a fully-processed temp folder (used for progress bar)
var WATCH_MAX_FILES = 10;

function watchDirChanged(){
  var val = (document.getElementById('watch-dir-input')||{value:''}).value.trim();
  _watchDir = val;
  if(val){
    _watchPaused = false;
    _watchSetPulse(true);
    document.getElementById('watch-status-text').textContent = 'Watching: ' + val;
    document.getElementById('watch-toggle-btn').textContent = '⏸ Pause';
    watchRefresh();
    _watchSchedule();
  } else {
    watchPause();
  }
}

function watchIntervalChanged(){
  var sel = document.getElementById('watch-interval-sel');
  _watchInterval = parseInt(sel.value) * 1000;
  if(!_watchPaused){ _watchSchedule(); }
}

function watchToggle(){
  if(_watchPaused){ watchResume(); } else { watchPause(); }
}
function watchPause(){
  _watchPaused = true;
  if(_watchTimer){ clearTimeout(_watchTimer); _watchTimer = null; }
  _watchSetPulse(false);
  document.getElementById('watch-toggle-btn').textContent = '▶ Resume';
  document.getElementById('watch-status-text').textContent =
    _watchDir ? 'Paused.' : 'Paused — enter a directory to start.';
}
function watchResume(){
  if(!_watchDir){ return; }
  _watchPaused = false;
  _watchSetPulse(true);
  document.getElementById('watch-toggle-btn').textContent = '⏸ Pause';
  document.getElementById('watch-status-text').textContent = 'Watching: ' + _watchDir;
  watchRefresh();
  _watchSchedule();
}
function _watchSetPulse(on){
  var p = document.getElementById('watch-pulse');
  if(p) p.classList.toggle('paused', !on);
}
function _watchSchedule(){
  if(_watchTimer){ clearTimeout(_watchTimer); }
  if(!_watchPaused){
    _watchTimer = setTimeout(function(){ watchRefresh(); _watchSchedule(); }, _watchInterval);
  }
}

function watchRefresh(){
  if(!_watchDir) return;
  var entW = (document.getElementById('entries_input')||{value:'f1 f2 f3'}).value.trim() || 'f1 f2 f3';
  fetch('/watch?user_dir=' + encodeURIComponent(_watchDir) + '&entries=' + encodeURIComponent(entW))
    .then(function(r){ return r.json(); })
    .then(function(d){ _renderWatch(d); })
    .catch(function(e){
      document.getElementById('watch-status-text').textContent = 'Fetch error: ' + e;
    });
}

function _renderWatch(d){
  var now = new Date().toLocaleTimeString();
  var upd = document.getElementById('watch-last-update');
  if(upd) upd.textContent = 'Updated ' + now;

  // ── file progress pane ────────────────────────────────────────────────────
  var tempBody = document.getElementById('watch-temp-body');
  var summary  = document.getElementById('watch-temp-summary');
  if(d.dir_error){
    tempBody.innerHTML = '<div class="scan-error">⚠ ' + _escHtml(d.dir_error) + '</div>';
  } else if(!d.temps || !d.temps.length){
    tempBody.innerHTML = '<div style="color:var(--text-muted);font-size:12px;padding:20px;text-align:center;">No numeric temperature folders found.</div>';
  } else {
    var done=0, proc=0, raw=0, emp=0;
    var stageOrder = d.relevant_stages || d.stage_order || ['load','link','copy','max','find','refine','transform','combine'];
    var stageShortW = {load:'load',link:'link',copy:'copy',max:'max',find:'find',refine:'refine',transform:'transform',combine:'combine'};
    var html = '';
    d.temps.forEach(function(t){
      var n = t.files;
      var pct, barClass, stClass, stLabel;
      if(n < 0){
        pct=0; barClass='prog-empty'; stClass='st-empty'; stLabel='no access';
      } else if(n === 0){
        emp++;
        pct=0; barClass='prog-empty'; stClass='st-empty'; stLabel='empty';
      } else if(n === 3){
        raw++;
        pct=Math.round(3/WATCH_MAX_FILES*100);
        barClass='prog-raw'; stClass='st-raw'; stLabel='raw data';
      } else if(n >= WATCH_MAX_FILES){
        done++;
        pct=100; barClass='prog-done'; stClass='st-done'; stLabel='done ✓';
      } else {
        proc++;
        pct=Math.round(n/WATCH_MAX_FILES*100);
        barClass='prog-mid'; stClass='st-proc'; stLabel='processing…';
      }
      // stage pills from .nxs (green=done, grey=todo)
      var stages = t.stages || {};
      var nDone = 0, nTotal = stageOrder.length;
      var pills = stageOrder.map(function(s){
        var isDone = !!stages[s];
        if(isDone) nDone++;
        var col = isDone
          ? 'background:rgba(34,197,94,.15);color:#22c55e;border:1px solid rgba(34,197,94,.3);'
          : 'background:rgba(148,163,184,.1);color:var(--text-muted);border:1px solid var(--border);';
        var mark = isDone ? '✓' : '○';
        return '<span style="font-size:9px;padding:1px 5px;border-radius:7px;'+col+'">'+mark+' '+stageShortW[s]+'</span>';
      }).join(' ');
      var nxsLabel = t.nxs ? t.nxs : '(no .nxs)';

      html += '<div class="temp-row" style="flex-direction:column;align-items:stretch;gap:5px;">'
            // top line: file-count bar
            + '<div style="display:flex;align-items:center;gap:10px;">'
            +   '<span class="temp-num">'+_escHtml(t.temp)+'K</span>'
            +   '<div class="prog-bar-wrap"><div class="prog-bar '+barClass+'" style="width:'+pct+'%"></div></div>'
            +   '<span class="temp-count">'+n+'</span>'
            +   '<span class="temp-status '+stClass+'">'+stLabel+'</span>'
            + '</div>'
            // bottom line: stage pills
            + '<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;padding-left:2px;">'
            +   '<span style="font-size:9px;color:var(--text-muted);min-width:90px;">'+_escHtml(nxsLabel)+' ('+nDone+'/'+nTotal+')</span>'
            +   '<span style="display:flex;gap:4px;flex-wrap:wrap;">'+pills+'</span>'
            + '</div>'
            + '</div>';
    });
    tempBody.innerHTML = html;
    if(summary) summary.textContent =
      done+' done · '+proc+' processing · '+raw+' raw · '+emp+' empty';
  }

  // ── qstat pane ────────────────────────────────────────────────────────────
  var qBody  = document.getElementById('watch-qstat-body');
  var qCount = document.getElementById('watch-qstat-count');
  if(d.qstat_error && !d.qstat){
    qBody.innerHTML = '<div class="scan-error" style="font-size:11px;">⚠ '+_escHtml(d.qstat_error)+'</div>';
    if(qCount) qCount.textContent = '';
  } else {
    var lines = d.qstat_lines || [];
    // Count job lines (skip header lines that start with job-ID or dashes)
    var jobLines = lines.filter(function(l){
      return l.trim() && !/^job-ID/i.test(l) && !/^-+/.test(l.trim());
    });
    if(qCount) qCount.textContent = jobLines.length + ' job' + (jobLines.length!==1?'s':'');
    // Colour-code: r=running(blue), qw=pending(gold), Eqw=error(red)
    var coloured = lines.map(function(line){
      var cls = '';
      // running = bright green, pending = amber, error = red; bold so each
      // state stands out clearly against the dark queue panel.
      if(/\br\b/.test(line))   cls = 'color:#4ade80;font-weight:700;';
      if(/\bqw\b/.test(line))  cls = 'color:#fbbf24;font-weight:700;';
      if(/\bEqw\b/.test(line)) cls = 'color:#f87171;font-weight:700;';
      // header row (job-ID prior name user …) and the dashed separator —
      // keep them clearly visible on the dark queue panel, not dim grey.
      if(/^-+/.test(line.trim()) || /^job-ID/i.test(line))
        cls = 'color:#e2e8f0;font-weight:700;';
      return cls ? '<span style="'+cls+'">'+_escHtml(line)+'</span>' : _escHtml(line);
    });
    qBody.innerHTML = '<pre class="qstat-pre">'+coloured.join(String.fromCharCode(10))+'</pre>';
    if(d.qstat_error){
      qBody.innerHTML += '<div style="font-size:10px;color:#f59e0b;margin-top:6px;">'+_escHtml(d.qstat_error)+'</div>';
    }
  }

  document.getElementById('watch-status-text').textContent = 'Watching: ' + d.user_dir + ' · refreshed ' + now;
}

// ── Sync watcher dir with USER_DIR (live, controlled by checkbox) ─────────────
function watchIsSynced(){
  var chk = document.getElementById('watch-sync-chk');
  return chk ? chk.checked : false;
}
function watchMirrorFromUserDir(){
  if(!watchIsSynced()) return;
  var ud = (document.getElementById('user_dir_input')||{value:''}).value.trim();
  var wd = document.getElementById('watch-dir-input');
  if(wd){
    wd.value = ud;
    watchDirChanged();
  }
}
function watchSyncToggle(){
  var wd = document.getElementById('watch-dir-input');
  var btn = document.getElementById('watch-browse-btn');
  if(watchIsSynced()){
    // Lock the field and mirror immediately
    if(wd){ wd.readOnly = true; wd.style.opacity = '.6'; }
    if(btn){ btn.style.opacity = '.7'; }   // browse still works (auto-unsyncs)
    watchMirrorFromUserDir();
  } else {
    // Unlock for manual entry
    if(wd){ wd.readOnly = false; wd.style.opacity = '1'; }
    if(btn){ btn.style.opacity = '1'; }
  }
}

// Browse for a watch directory. If currently synced to USER_DIR, switch off the
// sync first so the chosen folder sticks, then open the directory picker.
function watchBrowse(){
  var chk = document.getElementById('watch-sync-chk');
  if(chk && chk.checked){
    chk.checked = false;
    watchSyncToggle();
  }
  openDirPicker('watch-dir-input');
}

document.addEventListener('DOMContentLoaded', function(){
  var udInput = document.getElementById('user_dir_input');
  if(udInput){
    // Live mirror as the user types or selects a sample dir
    udInput.addEventListener('input', watchMirrorFromUserDir);
    udInput.addEventListener('change', watchMirrorFromUserDir);
  }
  // Apply initial locked state + mirror any pre-filled USER_DIR
  watchSyncToggle();
});

// ── Auto-Watch ────────────────────────────────────────────────────────────────
var _awTimer = null;
var _awRunning = false;
var _awInterval = 30000;

function _awMode(){
  var r = document.querySelector('input[name="aw_mode"]:checked');
  return r ? r.value : 'generate';
}
function awToggle(){
  if(_awRunning){ awStop(); } else { awStart(); }
}
function awStart(){
  var ud = (document.getElementById('aw-user-dir')||{value:''}).value.trim();
  if(!ud){
    alert('Enter a Sample directory (USER_DIR) for Auto-Watch first.');
    return;
  }
  _awRunning = true;
  _awInterval = parseInt((document.getElementById('aw-interval')||{value:'30'}).value)*1000;
  var p=document.getElementById('aw-pulse'); if(p) p.classList.remove('paused');
  document.getElementById('aw-toggle-btn').textContent = '⏸ Stop Auto-Watch';
  document.getElementById('aw-status-text').textContent = 'Watching ' + ud + ' …';
  awPoll();                  // immediate first poll
  _awSchedule();
}
function awStop(){
  _awRunning = false;
  if(_awTimer){ clearTimeout(_awTimer); _awTimer=null; }
  var p=document.getElementById('aw-pulse'); if(p) p.classList.add('paused');
  document.getElementById('aw-toggle-btn').textContent = '▶ Start Auto-Watch';
  document.getElementById('aw-status-text').textContent = 'Stopped.';
}
function _awSchedule(){
  if(_awTimer){ clearTimeout(_awTimer); }
  if(_awRunning){
    _awTimer = setTimeout(function(){ awPoll(); _awSchedule(); }, _awInterval);
  }
}
function awPoll(){
  var fd = new FormData();
  fd.append('user_dir', (document.getElementById('aw-user-dir')||{value:''}).value.trim());
  fd.append('chmod_dir', (document.getElementById('aw-chmod-dir')||{value:''}).value.trim());
  fd.append('script_dir', (document.getElementById('aw-script-dir')||{value:''}).value.trim());
  fd.append('mode', _awMode());
  fd.append('entries', (document.getElementById('entries_input')||{value:'f1 f2 f3'}).value.trim() || 'f1 f2 f3');
  fd.append('find_threshold', (document.getElementById('find_threshold_input')||{value:'80000'}).value || '80000');
  fd.append('remote_enable', (document.getElementById('remote-enable')||{checked:false}).checked ? '1' : '0');
  fd.append('remote_host', (document.getElementById('remote-host')||{value:''}).value.trim());
  fd.append('remote_user', (document.getElementById('remote-user')||{value:''}).value.trim());
  fd.append('remote_key', (document.getElementById('remote-key')||{value:''}).value.trim());
  fd.append('remote_pass', (document.getElementById('remote-pass')||{value:''}).value);
  fetch('/autowatch_poll', {method:'POST', body:fd})
    .then(function(r){return r.json();})
    .then(function(d){ _awRender(d); _jhMaybe(); })
    .catch(function(e){
      document.getElementById('aw-status-text').textContent = 'Poll error: ' + e;
    });
}
function awReset(){
  if(!confirm('Reset the handled list? Temperatures already processed can be auto-processed again.')) return;
  fetch('/autowatch_reset', {method:'POST'}).then(function(){
    var b=document.getElementById('aw-log-body');
    if(b) b.innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">Handled list cleared.</div>';
  });
}
function _awRender(d){
  var now = new Date().toLocaleTimeString();
  var upd = document.getElementById('aw-last-update');
  if(upd) upd.textContent = 'Polled ' + now;
  if(d.error){
    document.getElementById('aw-status-text').textContent = '⚠ ' + d.error;
    return;
  }
  var nNew = (d.actions||[]).length;
  document.getElementById('aw-status-text').textContent =
    'Watching · stage=' + (d.stage||'?') + (d.has_parent?' (parent)':' (no parent)')
    + ' · ' + (nNew? (nNew+' new this cycle') : 'no new temps')
    + ' · ' + (d.handled_count||0) + ' handled total · ' + now;

  var log = d.log || [];
  var body = document.getElementById('aw-log-body');
  if(!log.length){
    body.innerHTML = '<div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">No activity yet — waiting for raw scans to appear.</div>';
    return;
  }
  var html = '';
  // newest first
  for(var i=log.length-1;i>=0;i--){
    var e = log[i];
    var col = e.ok ? '#22c55e' : '#ef4444';
    var mark = e.ok ? '✓' : '✗';
    html += '<div style="display:flex;align-items:center;gap:10px;padding:5px 12px;border-bottom:1px solid var(--border);font-size:12px;">'
          + '<span style="font-family:var(--mono);color:var(--text-muted);min-width:64px;">'+_escHtml(e.time)+'</span>'
          + '<span class="chip chip-gold" style="font-size:11px;">'+_escHtml(e.temp)+'K</span>'
          + '<span style="font-size:10px;color:var(--text-muted);min-width:66px;">'+_escHtml(e.stage)+'</span>'
          + '<span style="font-size:11px;color:var(--text-muted);">'+_escHtml(e.action)+'</span>'
          + '<span style="margin-left:auto;color:'+col+';font-size:11px;">'+mark+' '+_escHtml(e.detail||'')+'</span>'
          + '</div>';
  }
  body.innerHTML = html;
}

// ── Auto-Submit (parent-aware, separate from Auto-Watch) ──────────────────────
var _asTimer = null;
var _asRunning = false;
var _asInterval = 30000;

function _asMode(){
  var r = document.querySelector('input[name="as_mode"]:checked');
  return r ? r.value : 'generate';
}
function asToggle(){
  if(_asRunning){ asStop(); } else { asStart(); }
}
function asStart(){
  var ud = (document.getElementById('as-user-dir')||{value:''}).value.trim();
  if(!ud){
    alert('Enter a Sample directory (USER_DIR) for Auto-Submit first.');
    return;
  }
  _asRunning = true;
  _asInterval = parseInt((document.getElementById('as-interval')||{value:'30'}).value)*1000;
  var p=document.getElementById('as-pulse'); if(p) p.classList.remove('paused');
  document.getElementById('as-toggle-btn').textContent = '⏸ Stop Auto-Submit';
  document.getElementById('as-status-text').textContent = 'Auto-submitting from ' + ud + ' …';
  asPoll();                  // immediate first poll
  _asSchedule();
}
function asStop(){
  _asRunning = false;
  if(_asTimer){ clearTimeout(_asTimer); _asTimer=null; }
  var p=document.getElementById('as-pulse'); if(p) p.classList.add('paused');
  document.getElementById('as-toggle-btn').textContent = '▶ Start Auto-Submit';
  document.getElementById('as-status-text').textContent = 'Stopped.';
}
function _asSchedule(){
  if(_asTimer){ clearTimeout(_asTimer); }
  if(_asRunning){
    _asTimer = setTimeout(function(){ asPoll(); _asSchedule(); }, _asInterval);
  }
}
function asPoll(){
  var fd = new FormData();
  fd.append('user_dir', (document.getElementById('as-user-dir')||{value:''}).value.trim());
  fd.append('chmod_dir', (document.getElementById('as-chmod-dir')||{value:''}).value.trim());
  fd.append('script_dir', (document.getElementById('as-script-dir')||{value:''}).value.trim());
  fd.append('parent_temp', (document.getElementById('as-parent-temp')||{value:'300'}).value.trim() || '300');
  fd.append('auto_parent', (document.getElementById('as-auto-parent')||{checked:true}).checked ? '1' : '0');
  fd.append('mode', _asMode());
  fd.append('entries', (document.getElementById('entries_input')||{value:'f1 f2 f3'}).value.trim() || 'f1 f2 f3');
  fd.append('find_threshold', (document.getElementById('find_threshold_input')||{value:'80000'}).value || '80000');
  fd.append('remote_enable', (document.getElementById('remote-enable')||{checked:false}).checked ? '1' : '0');
  fd.append('remote_host', (document.getElementById('remote-host')||{value:''}).value.trim());
  fd.append('remote_user', (document.getElementById('remote-user')||{value:''}).value.trim());
  fd.append('remote_key', (document.getElementById('remote-key')||{value:''}).value.trim());
  fd.append('remote_pass', (document.getElementById('remote-pass')||{value:''}).value);
  fetch('/autosubmit_poll', {method:'POST', body:fd})
    .then(function(r){return r.json();})
    .then(function(d){ _asRender(d); _jhMaybe(); })
    .catch(function(e){
      document.getElementById('as-status-text').textContent = 'Poll error: ' + e;
    });
}
function asReset(){
  if(!confirm('Reset the Auto-Submit handled list? Temperatures already processed can be auto-submitted again.')) return;
  fetch('/autosubmit_reset', {method:'POST'}).then(function(){
    var b=document.getElementById('as-log-body');
    if(b) b.innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">Handled list cleared.</div>';
  });
}
function _asRender(d){
  var now = new Date().toLocaleTimeString();
  var upd = document.getElementById('as-last-update');
  if(upd) upd.textContent = 'Polled ' + now;
  if(d.error){
    document.getElementById('as-status-text').textContent = '⚠ ' + d.error;
    return;
  }
  var nNew = (d.actions||[]).length;
  document.getElementById('as-status-text').textContent =
    'Auto-submit · stage=' + (d.stage||'?') + (d.has_parent?' (parent)':' (no parent)')
    + ' · skip ' + (d.parent_temp||'300') + 'K' + (d.parent_source==='auto' ? ' (auto)' : ' (manual)')
    + ' · ' + (nNew? (nNew+' new this cycle') : 'no new temps')
    + ' · ' + (d.handled_count||0) + ' handled total · ' + now;

  var ph = document.getElementById('as-parent-hint');
  if(ph){
    if(d.parent_source==='auto' && d.parent_detected){
      ph.textContent = 'Auto-detected parent = ' + d.parent_detected + 'K (from *_parent.nxs). This folder is skipped.';
    } else {
      ph.textContent = 'No parent file detected yet — using fallback ' + (d.parent_temp||'300') + 'K.';
    }
  }

  var log = d.log || [];
  var body = document.getElementById('as-log-body');
  if(!log.length){
    body.innerHTML = '<div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">No activity yet — waiting for temperatures to be ready.</div>';
    return;
  }
  var html = '';
  // newest first
  for(var i=log.length-1;i>=0;i--){
    var e = log[i];
    var col = e.ok ? '#22c55e' : '#ef4444';
    var mark = e.ok ? '✓' : '✗';
    html += '<div style="display:flex;align-items:center;gap:10px;padding:5px 12px;border-bottom:1px solid var(--border);font-size:12px;">'
          + '<span style="font-family:var(--mono);color:var(--text-muted);min-width:64px;">'+_escHtml(e.time)+'</span>'
          + '<span class="chip chip-gold" style="font-size:11px;">'+_escHtml(e.temp)+'K</span>'
          + '<span style="font-size:10px;color:var(--text-muted);min-width:66px;">'+_escHtml(e.stage)+'</span>'
          + '<span style="font-size:11px;color:var(--text-muted);">'+_escHtml(e.action)+'</span>'
          + '<span style="margin-left:auto;color:'+col+';font-size:11px;">'+mark+' '+_escHtml(e.detail||'')+'</span>'
          + '</div>';
  }
  body.innerHTML = html;
}

/* ── Jobs needing attention (error / held / suspended / gone-no-output) ──────── */
function _jhSource(){
  // Scope 'gone, no output' rows to whichever sample dir is in play.
  var a=(document.getElementById('as-user-dir')||{value:''}).value.trim();
  if(a) return a;
  var w=(document.getElementById('aw-user-dir')||{value:''}).value.trim();
  if(w) return w;
  return (document.getElementById('user_dir_input')||{value:''}).value.trim();
}
function jhRefresh(){
  var fd=new FormData();
  fd.append('user_dir', _jhSource());
  fetch('/job_health', {method:'POST', body:fd})
    .then(function(r){return r.json();})
    .then(function(d){ _jhRender(d); })
    .catch(function(e){
      var s=document.getElementById('jh-status');
      if(s) s.textContent='Could not check job health: '+e;
    });
}
function _jhMaybe(){
  var cb=document.getElementById('jh-autorefresh');
  if(cb && cb.checked) jhRefresh();
}
function _jhBadge(cat){
  var map={'error':['#dc2626','SGE error'],'held':['#d97706','Held'],
           'suspended':['#7c3aed','Suspended'],'gone-no-output':['#0891b2','Gone · no output']};
  var m=map[cat]||['#64748b', cat||'issue'];
  return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;'
       + 'font-weight:600;color:#fff;background:'+m[0]+';white-space:nowrap;">'+_escHtml(m[1])+'</span>';
}
function _jhRender(d){
  var now=new Date().toLocaleTimeString();
  var upd=document.getElementById('jh-last-update'); if(upd) upd.textContent='Checked '+now;
  var dot=document.getElementById('jh-dot');
  var status=document.getElementById('jh-status');
  var wrap=document.getElementById('jh-table-wrap');
  var jobs=(d&&d.jobs)||[];
  if(d && d.qstat_ok===false){
    if(status) status.textContent='⚠ '+(d.error||'qstat unavailable.');
  }
  if(!jobs.length){
    if(dot) dot.style.background='#16a34a';
    if(status) status.textContent=(d&&d.qstat_ok===false)
      ? ((d.error||'qstat unavailable')+' — only locally-tracked jobs can be checked.')
      : 'All clear — no jobs in an error, held, suspended, or gone-no-output state. ('+now+')';
    if(wrap) wrap.innerHTML='';
    return;
  }
  if(dot) dot.style.background='#dc2626';
  if(status) status.textContent=jobs.length+' job'+(jobs.length!==1?'s':'')+' need attention — full details below. ('+now+')';
  var h='<table style="width:100%;border-collapse:collapse;font-size:12px;">'
      + '<thead><tr style="text-align:left;color:var(--text-muted);border-bottom:1px solid var(--border);">'
      + '<th style="padding:6px 8px;">Issue</th><th style="padding:6px 8px;">Job ID</th>'
      + '<th style="padding:6px 8px;">Temp</th><th style="padding:6px 8px;">Name</th>'
      + '<th style="padding:6px 8px;">User</th><th style="padding:6px 8px;">State</th>'
      + '<th style="padding:6px 8px;">Queue</th><th style="padding:6px 8px;">Submitted</th>'
      + '<th style="padding:6px 8px;">Details</th></tr></thead><tbody>';
  for(var i=0;i<jobs.length;i++){
    var j=jobs[i];
    var nm=j.full_name||j.name||'';
    var detail=(j.issue||'')+((j.folder)?('  ['+j.folder+']'):'');
    h+='<tr style="border-bottom:1px solid var(--border);">'
     + '<td style="padding:6px 8px;">'+_jhBadge(j.category)+'</td>'
     + '<td style="padding:6px 8px;font-family:var(--mono);">'+_escHtml(j.id||'')+'</td>'
     + '<td style="padding:6px 8px;">'+(j.temp?(_escHtml(j.temp)+'K'):'—')+'</td>'
     + '<td style="padding:6px 8px;font-family:var(--mono);">'+_escHtml(nm||'—')+'</td>'
     + '<td style="padding:6px 8px;">'+_escHtml(j.user||'—')+'</td>'
     + '<td style="padding:6px 8px;font-family:var(--mono);color:var(--red);">'+_escHtml(j.state||'')+'</td>'
     + '<td style="padding:6px 8px;font-family:var(--mono);">'+_escHtml(j.queue||'—')+'</td>'
     + '<td style="padding:6px 8px;color:var(--text-muted);">'+_escHtml(j.submit||'—')+'</td>'
     + '<td style="padding:6px 8px;color:var(--text-muted);">'+_escHtml(detail)+'</td>'
     + '</tr>';
  }
  h+='</tbody></table>';
  if(wrap) wrap.innerHTML=h;
}
document.addEventListener('DOMContentLoaded', function(){ try{ jhRefresh(); }catch(e){} });

// ── Tab shell ──
function showTab(id){
  document.querySelectorAll('.tabpanel').forEach(function(p){p.classList.remove('active');});
  document.querySelectorAll('.tab-btn').forEach(function(b){b.classList.remove('active');});
  var panel=document.getElementById('tab-'+id); if(panel) panel.classList.add('active');
  var btn=document.querySelector('.tab-btn.tab-'+id); if(btn) btn.classList.add('active');
  if(id==='health'){ try{ jhRefresh(); }catch(e){} }
}
function exactGenerate(){
  scanSyncSelected();
  var sel = (document.getElementById('autoscan_selected_val')||{value:''}).value.trim();
  if(!sel){ alert('No temperatures selected. Click \u201cScan Now\u201d, tick the temperatures you want, then run.'); return; }
  var r = document.querySelector('input[name="scan_gen_mode"]:checked');
  var mode = (r && r.value === 'submit') ? 'submit' : 'preview';
  autoscanGenerate(mode);
}
document.addEventListener('DOMContentLoaded', function(){
  {% if autoscan_results or autoscan_raw_skipped %}showTab('scan');{% elif errors or script_text or (job_output is not none) or (pre_chmod_output is not none) %}showTab('submit');{% endif %}
});
</script>
</head>
<body>

<header>
  <div class="hdr-left">
    <div class="logo" id="hdr-logo">
      <img src="/logo" alt="CHESS"
           onerror="var d=document.getElementById('hdr-logo');d.classList.add('fallback');d.innerHTML='NX';">
    </div>
    <div>
      <div class="hdr-title">NxRefine Job Submission CLASSE Cluster (lnx201) for Quantum Materials Beamline</div>
      <div class="hdr-sub">CHESS · Cornell · SGE Cluster &nbsp;|&nbsp; Developed by QM2 Beamline Scientist</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:10px;">
    <span class="badge badge-blue">{{ default_queue }}</span>
    {% if history|length > 0 %}
    <span class="badge badge-gold">{{ history|length }} job{% if history|length!=1 %}s{% endif %} submitted</span>
    {% endif %}
    <button class="theme-btn" onclick="toggleTheme()">&#9790; Theme</button>
  </div>
</header>

<div class="page">

  <div class="tabbar" id="nx-tabbar">
    <button type="button" class="tab-btn tab-general active" onclick="showTab('general')"><span class="tab-ico">&#128203;</span> General</button>
    <button type="button" class="tab-btn tab-submit" onclick="showTab('submit')"><span class="tab-ico">&#128228;</span> Submit Job</button>
    <button type="button" class="tab-btn tab-scan" onclick="showTab('scan')"><span class="tab-ico">&#128269;</span> Exact .nxs Scan</button>
    <button type="button" class="tab-btn tab-watch" onclick="showTab('watch')"><span class="tab-ico">&#128225;</span> Live Watcher + SGE</button>
    <button type="button" class="tab-btn tab-auto" onclick="showTab('auto')"><span class="tab-ico">&#9881;</span> Auto-Submit</button>
    <button type="button" class="tab-btn tab-health" onclick="showTab('health')"><span class="tab-ico">&#9888;</span> Jobs Needing Attention</button>
  </div>

  <!-- ═══ GENERAL TAB — shared settings used by every tab (Submit, Exact scan,
       Auto-Submit). Inputs bind to the Submit form via form="mainform" so they
       submit together no matter which tab is showing. ═══ -->
  <div class="tabpanel active" id="tab-general">

    <div class="card">
      <div class="card-header">
        <div class="card-title"><div class="dot"></div>General settings &mdash; shared across all tabs</div>
      </div>
      <p class="hint" style="margin:0 0 14px;">Set these once. The Submit Job tab, the Exact&nbsp;.nxs Scan tab and Auto-Submit all use the values below when they generate or submit scripts. The sample directory and frame entries are set per job in the <strong>Submit Job</strong> tab.</p>

      <!-- Queue options -->
      <div class="field">
        <label class="field-label">Queue / resources</label>
        <div class="row">
          <div class="field">
            <label class="field-label">Queue</label>
            <input type="text" form="mainform" name="q_queue" value="{{ q_queue|default('all.q') }}" style="width:160px;">
          </div>
          <div class="field">
            <label class="field-label">mem_free</label>
            <input type="text" form="mainform" name="q_mem" value="{{ q_mem|default('300G') }}" style="width:90px;">
          </div>
          <div class="field">
            <label class="field-label">Parallel env</label>
            <input type="text" form="mainform" name="q_pe" value="{{ q_pe|default('sge_pe') }}" style="width:110px;">
          </div>
          <div class="field">
            <label class="field-label">Cores</label>
            <input type="number" form="mainform" name="q_cores" value="{{ q_cores|default('12') }}" style="width:80px;" min="1">
          </div>
        </div>
      </div>

      <!-- Email notification -->
      <div class="field">
        <label class="field-label">Email notification <span style="font-weight:400;text-transform:none;">(optional)</span></label>
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <input type="text" form="mainform" name="notify_email" value="{{ notify_email|default('') }}"
                 placeholder="your@email.com" style="width:280px;">
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" form="mainform" name="notify_end"   value="1" {% if notify_end  !='0' %}checked{% endif %}> Job end
          </label>
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" form="mainform" name="notify_abort" value="1" {% if notify_abort=='1' %}checked{% endif %}> Abort / error
          </label>
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" form="mainform" name="notify_begin" value="1" {% if notify_begin=='1' %}checked{% endif %}> Job start
          </label>
        </div>
        <p class="hint">Adds <code>#$ -M</code> / <code>#$ -m</code> SGE directives to the script header.</p>
      </div>

      <div class="divider"></div>

      <!-- Script / chmod paths -->
      <div class="field">
        <label class="field-label">Script output directory</label>
        <div style="display:flex;gap:8px;align-items:center;max-width:740px;">
          <input type="text" form="mainform" id="script_dir_input" name="script_dir" value="{{ script_dir }}"
                 placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/script/after/"
                 style="flex:1;">
          <button type="button" class="browse-btn" onclick="openDirPicker('script_dir_input')">
            &#128193; Browse
          </button>
        </div>
        <p class="hint">Where to save the <code>.sh</code> file. Leave blank to save alongside USER_DIR (the proposal-level <code>scripts/</code> folder).</p>
      </div>

      <div class="field">
        <label class="field-label">chmod path <span style="font-weight:400;text-transform:none;">(optional &mdash; auto-filled from USER_DIR)</span></label>
        <input type="text" form="mainform" id="chmod_path_input" name="chmod_path" value="{{ chmod_path }}"
               placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/"
               style="max-width:620px;">
        <p class="hint">Runs <code>chmod -R 777</code> on this path at the end of the script. Auto-filled to the PI folder when you browse for USER_DIR.</p>
      </div>
    </div>

    <!-- Pre-submission chmod (independent + auto-with-submit) -->
    <div class="card">
      <div class="card-header">
        <div class="card-title"><div class="dot"></div>Pre-submission chmod</div>
      </div>
      <div class="field" style="border:1px solid var(--border);border-radius:8px;padding:14px 16px;background:rgba(217,119,6,.06);">
        <label class="field-label" style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
          <input type="checkbox" form="mainform" id="pre_chmod_enable_input" name="pre_chmod_enable" value="1"
                 {% if pre_chmod_enable=='1' %}checked{% endif %}>
          Pre-submission chmod <span style="font-weight:400;text-transform:none;">(run <code>chmod -R 777</code> before <code>qsub</code>)</span>
        </label>
        <div style="display:flex;gap:8px;align-items:center;max-width:900px;">
          <input type="text" form="mainform" id="pre_chmod_path_input" name="pre_chmod_path" value="{{ pre_chmod_path }}"
                 placeholder="/nfs/chess/id4baux/2026-3/hallas-4894-c/"
                 style="flex:1;">
          <button type="button" class="browse-btn" onclick="openDirPicker('pre_chmod_path_input')">
            &#128193; Browse
          </button>
          <button type="submit" form="mainform" class="btn" style="background:linear-gradient(135deg,#b45309,#d97706);color:#fff;white-space:nowrap;padding:9px 18px;box-shadow:0 2px 12px rgba(217,119,6,.25);"
                  onclick="document.getElementById('form-action').value='chmod'">
            &#128274; Run chmod now
          </button>
        </div>
        <p class="hint">Two ways to use this: <strong>(1) tick the box</strong> and it runs automatically right before the job is submitted (with <em>Generate &amp; Submit</em>); or <strong>(2) click “Run chmod now”</strong> to run it immediately on its own, without generating or submitting a job. Path must be under <code>/nfs/chess/</code>.</p>
      </div>
    </div>

    <!-- Remote submission: submit qsub as a different user over SSH -->
    <div class="card">
      <div class="card-header">
        <div class="card-title"><div class="dot"></div>Submit as a different user (remote login)</div>
      </div>
      <div class="field" style="border:1px solid rgba(250,204,21,.35);border-radius:10px;padding:14px 16px;background:rgba(250,204,21,.04);">
        <label style="display:flex;align-items:center;gap:8px;font-size:12px;font-weight:600;cursor:pointer;text-transform:uppercase;letter-spacing:.4px;color:var(--text-muted);">
          <input type="checkbox" form="mainform" id="remote-enable" name="remote_enable" value="1" {% if remote_enable=='1' %}checked{% endif %}>
          Submit as a different user (remote login)
        </label>
        <p class="hint" style="margin-top:8px;">
          Set who the jobs are submitted as, <strong>then fill in the job details in the other tabs</strong>.
          Runs <code>qsub</code> over SSH on the cluster so the jobs count against <em>that</em>
          account's submission quota. The scripts already live on the shared
          <code>/nfs/chess</code> filesystem, so nothing is copied &mdash; the same
          <code>qsub &lt;script&gt;</code> just runs under the other login.
          <strong>This cluster uses Duo two-factor, so a password alone will not log in.</strong>
          Use an <strong>SSH key</strong>: leave the password blank and set up a key once
          (see below). You can point at a specific key file, or leave it blank to use your
          default <code>~/.ssh</code> keys / ssh-agent.
        </p>
        <div class="row" style="margin-top:4px;">
          <div class="field">
            <label class="field-label">SSH host</label>
            <input type="text" form="mainform" id="remote-host" name="remote_host"
                   value="{{ remote_host|default('lnx201.classe.cornell.edu') }}" style="width:260px;">
          </div>
          <div class="field">
            <label class="field-label">Username</label>
            <input type="text" form="mainform" id="remote-user" name="remote_user" autocomplete="username"
                   value="{{ remote_user|default('') }}" placeholder="CLASSE login" style="width:170px;">
          </div>
          <div class="field">
            <label class="field-label">Private key file <span style="font-weight:400;text-transform:none;">(optional)</span></label>
            <input type="text" form="mainform" id="remote-key" name="remote_key"
                   value="{{ remote_key|default('') }}" placeholder="~/.ssh/id_ed25519" style="width:220px;">
          </div>
          <div class="field">
            <label class="field-label">Password <span style="font-weight:400;text-transform:none;">(no Duo = won't work)</span></label>
            <input type="password" form="mainform" id="remote-pass" name="remote_pass" autocomplete="off"
                   placeholder="use SSH key instead" style="width:170px;">
          </div>
        </div>
        <p class="hint" style="margin-top:6px;">
          <strong>One-time key setup</strong> (run in a terminal; the copy step asks for your
          Duo login once):
          <code>ssh-keygen -t ed25519</code> &nbsp;then&nbsp;
          <code>ssh-copy-id {{ remote_user|default('USER') }}@{{ remote_host|default('lnx201.classe.cornell.edu') }}</code>.
          After that, submissions here log in with no password and no Duo prompt.
        </p>
      </div>
    </div>

  </div>

  <div class="tabpanel" id="tab-submit">


  <!-- ── NxRefine Job Configuration at Quantum Materials (QM2) Beamline ───────────────────────────────────────────────── -->
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot"></div>NxRefine Job Configuration for Quantum Materials (QM2) Beamline</div>
    </div>

    <form method="POST" action="/" id="mainform">
      <input type="hidden" name="action" id="form-action" value="preview">
      <input type="hidden" name="temp_mode" id="temp_mode_val" value="{{ temp_mode|default('single') }}">
      <input type="hidden" name="autoscan_selected" id="autoscan_selected_val" value="">
      <input type="hidden" name="autoscan_exact" id="autoscan_exact_val" value="0">

      <!-- Sample directory -->
      <div class="field">
        <label class="field-label">Sample directory (USER_DIR)</label>
        <div style="display:flex;gap:8px;align-items:center;max-width:740px;">
          <input type="text" id="user_dir_input" name="user_dir" value="{{ user_dir }}"
                 placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/nxrefine/SAMPLE/SAMPLE_b1_s1/"
                 style="flex:1;" oninput="updatePathPreview()">
          <button type="button" class="browse-btn" onclick="openDirPicker('user_dir_input')">
            &#128193; Browse
          </button>
        </div>
        <p class="hint">Rooted at <code>{{ root_path }}</code> &mdash; TEMP is appended as a subdirectory name</p>
        <div id="path-preview" class="path-preview"></div>
      </div>

      <!-- Entries -->
      <div class="field">
        <label class="field-label">Frame entries</label>
        <input type="text" name="entries" id="entries_input" value="{{ entries|default('f1 f2 f3') }}"
               style="width:200px;" placeholder="f1 f2 f3">
        <p class="hint">Space-separated list of entries to process</p>
      </div>

      <div class="divider"></div>

      <!-- Temperature -->
      <div class="field">
        <label class="field-label">Temperature(s)</label>
        <div class="temp-modes">
          <button type="button" class="mode-btn" id="modeBtn_single" onclick="setTempMode('single')">Single</button>
          <button type="button" class="mode-btn" id="modeBtn_range"  onclick="setTempMode('range')">Range</button>
          <button type="button" class="mode-btn" id="modeBtn_list"   onclick="setTempMode('list')">List</button>
        </div>
        <div id="tblock_single" style="display:none;">
          <input type="number" id="temp_single_input" name="temp_single"
                 value="{{ temp_single|default('') }}"
                 style="width:130px;" placeholder="e.g. 91" oninput="updatePathPreview()">
          <span class="hint" style="display:inline-block;margin-left:8px;">Single temperature value</span>
        </div>
        <div id="tblock_range" style="display:none;">
          <div class="row" style="gap:14px;">
            <div class="field">
              <label class="field-label">Start</label>
              <input type="number" id="temp_start_input" name="temp_start"
                     value="{{ temp_start|default('') }}" style="width:110px;"
                     placeholder="e.g. 19" oninput="updatePathPreview()">
            </div>
            <div class="field">
              <label class="field-label">End</label>
              <input type="number" name="temp_end" value="{{ temp_end|default('') }}" style="width:110px;" placeholder="e.g. 28">
            </div>
            <div class="field">
              <label class="field-label">Step</label>
              <input type="number" name="temp_step" value="{{ temp_step|default('2') }}" style="width:110px;">
            </div>
          </div>
          <p class="hint">Produces: start, start+step, start+2·step, … up to end</p>
        </div>
        <div id="tblock_list" style="display:none;">
          <input type="text" id="temp_list_input" name="temp_list"
                 value="{{ temp_list|default('100,105,110') }}"
                 style="width:360px;" placeholder="100,105,110"
                 oninput="updatePathPreview()">
          <p class="hint">Comma or space separated values</p>
        </div>
      </div>

      <!-- Processing steps -->
      <div class="field">
        <label class="field-label">Processing steps</label>
        <!-- Quick presets -->
        <div class="preset-bar">
          <span style="font-size:11px;color:var(--text-muted);align-self:center;margin-right:2px;">Quick&nbsp;select:</span>
          <button type="button" class="preset-btn" data-preset="parent_avail"
                  onclick="applyPreset('parent_avail')">&#9654; Parent file (available)</button>
          <button type="button" class="preset-btn" data-preset="parent_not_avail"
                  onclick="applyPreset('parent_not_avail')">&#9654; Parent file (not available)</button>
          <button type="button" class="preset-btn" data-preset="after_parent"
                  onclick="applyPreset('after_parent')">&#9654; After parent file available</button>
        </div>
        <div class="steps-grid">

          <label class="step-card">
            <input type="checkbox" id="step_load" name="step_load" value="1"
                   {% if step_load!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Load</div>
              <div class="step-cmd">--load --overwrite</div>
              <div class="step-desc">Stack / load raw .cbf frames into f1 / f2 / f3</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_copy" name="step_link_copy" value="1"
                   {% if step_link_copy!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Link + Copy + Max</div>
              <div class="step-cmd">--link --copy --max --overwrite</div>
              <div class="step-desc">Link, copy orientation from parent file, compute max</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_only" name="step_link_only" value="1"
                   {% if step_link_only=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Link only + Max</div>
              <div class="step-cmd">--link --max --overwrite</div>
              <div class="step-desc">Use when no parent file is available</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_ow" name="step_link_ow" value="1"
                   {% if step_link_ow=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Link only (overwrite)</div>
              <div class="step-cmd">--link --overwrite</div>
              <div class="step-desc">Re-link with overwrite (no max, no copy)</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_copy_only" name="step_copy_only" value="1"
                   {% if step_copy_only=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Copy only</div>
              <div class="step-cmd">--copy --overwrite</div>
              <div class="step-desc">Re-run copy if it wasn't completed</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_find" name="step_find" value="1"
                   {% if step_find!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Find peaks</div>
              <div class="step-cmd">nxfind &nbsp;-t &nbsp;
                <input type="number" name="find_threshold"
                       value="{{ find_threshold|default('80000') }}"
                       style="width:90px;padding:2px 6px;font-size:11px;display:inline-block;"
                       min="1000" step="1000"
                       onclick="event.stopPropagation()" onchange="event.stopPropagation()">
              </div>
              <div class="step-desc">Find Bragg peaks above threshold</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_refine" name="step_refine" value="1"
                   {% if step_refine!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Refine</div>
              <div class="step-cmd">--refine --overwrite</div>
              <div class="step-desc">Refine the orientation matrix</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_transform" name="step_transform" value="1"
                   {% if step_transform!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Transform + Combine</div>
              <div class="step-cmd">--transform --combine --regular</div>
              <div class="step-desc">HKL transform and combine all frames</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_pdf" name="step_pdf" value="1"
                   {% if step_pdf=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">3D PDF</div>
              <div class="step-cmd">--pdf --regular</div>
              <div class="step-desc">Generate 3D pair distribution function (slow)</div>
            </div>
          </label>

        </div>
      </div>

      <div class="divider"></div>

      <p class="hint" style="margin:0 0 10px;">Queue/resources, email, script output and chmod paths are set in the <strong>General</strong> tab and apply here too.</p>

      <!-- Action buttons -->
      <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;">
        <button type="submit" class="btn btn-primary"
                onclick="return mainGenerate('preview')">
          &#128196; Generate Script
        </button>
        <button type="submit" class="btn btn-submit"
                onclick="return mainGenerate('submit')">
          &#9654; Generate &amp; Submit Job
        </button>
      </div>
    </form>
  </div>

  <!-- ── Validation errors ────────────────────────────────────────────────── -->
  {% if errors %}
  <div class="card" style="border-color:rgba(239,68,68,.35);">
    {% for e in errors %}<div class="alert-error" style="margin-bottom:6px;">&#9888; {{ e }}</div>{% endfor %}
  </div>
  {% endif %}

  <!-- ── Generated script preview ─────────────────────────────────────────── -->
  {% if script_text %}
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot dot-gold"></div>Generated Script</div>
      <div style="display:flex;align-items:center;gap:10px;">
        <code style="font-size:11px;">{{ script_path }}</code>
        <button id="copy-btn" class="btn btn-clear" style="padding:4px 12px;font-size:11px;" onclick="copyScript()">
          Copy Script
        </button>
      </div>
    </div>
    <div class="script-pre" id="script-pre-content">{{ script_text }}</div>
    <div class="qsub-bar">
      <span class="qsub-label">qsub command:</span>
      <span style="color:#38bdf8;">{{ qsub_cmd }}</span>
    </div>
  </div>
  {% endif %}

  <!-- ── Submission result ─────────────────────────────────────────────────── -->
  {% if pre_chmod_output is not none %}
  <div class="card" style="border-color:{% if pre_chmod_ok %}rgba(34,197,94,.35){% else %}rgba(239,68,68,.35){% endif %};">
    <div class="card-header">
      <div class="card-title">
        <div class="dot {% if pre_chmod_ok %}dot-green{% else %}dot-red{% endif %}"></div>
        Pre-submission chmod {% if pre_chmod_ok %}Successful{% else %}Failed{% endif %}
      </div>
    </div>
    {% if pre_chmod_ok %}
    <p class="alert-success">&#10003; {{ pre_chmod_output }}</p>
    {% else %}
    <p class="alert-error">{{ pre_chmod_output }}</p>
    {% endif %}
  </div>
  {% endif %}

  {% if job_output is not none %}
  <div class="card" style="border-color:{% if job_ok %}rgba(34,197,94,.35){% else %}rgba(239,68,68,.35){% endif %};">
    <div class="card-header">
      <div class="card-title">
        <div class="dot {% if job_ok %}dot-green{% else %}dot-red{% endif %}"></div>
        Submission {% if job_ok %}Successful{% else %}Failed{% endif %}
      </div>
    </div>
    {% if job_ok %}
    <p class="alert-success">&#10003; {{ job_output }}</p>
    {% else %}
    <p class="alert-error">{{ job_output }}</p>
    {% endif %}
    {% if job_stderr %}
    <pre style="margin-top:10px;font-size:11px;color:var(--text-muted);">{{ job_stderr }}</pre>
    {% endif %}
  </div>
  {% endif %}

  <!-- ── Job history ───────────────────────────────────────────────────────── -->
  {% if history %}
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot"></div>Job History <span class="badge badge-blue" style="margin-left:6px;">{{ history|length }}</span></div>
      <form method="POST" action="/clear_history" style="margin:0;">
        <button type="submit" class="btn btn-clear" style="padding:4px 12px;font-size:11px;">Clear</button>
      </form>
    </div>
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Script</th>
          <th>Temperatures</th>
          <th>Steps</th>
          <th>Result</th>
        </tr>
      </thead>
      <tbody>
        {% for j in history|reverse %}
        <tr>
          <td style="white-space:nowrap;color:var(--text-muted);">{{ j.time }}</td>
          <td style="font-size:11px;word-break:break-all;max-width:220px;">{{ j.script_name }}</td>
          <td style="white-space:nowrap;">{{ j.temps }}</td>
          <td style="font-size:11px;color:var(--text-muted);">{{ j.steps }}</td>
          <td>
            {% if j.ok %}
            <span class="job-ok">&#10003; {{ j.job_id }}</span>
            {% else %}
            <span class="job-fail">&#10007; failed</span>
            {% if j.detail %}<div style="font-size:11px;color:var(--text-muted);margin-top:3px;word-break:break-word;max-width:420px;white-space:normal;">{{ j.detail }}</div>{% endif %}
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% endif %}

    </div><!-- /tab-submit -->

  <div class="tabpanel" id="tab-scan">

  <!-- ── Exact .nxs Scan controls ── -->
  <div class="card" style="border-color:rgba(245,158,11,.4);">
    <div class="card-header">
      <div class="card-title"><div class="dot dot-gold"></div>Exact .nxs Folder Scan</div>
      <code style="font-size:11px;color:var(--text-muted);">reads each temperature's .nxs — resume from first incomplete stage</code>
    </div>
    <div style="display:flex;flex-direction:column;gap:12px;">
      <p class="hint" style="margin:0;">
        Scans the sample directory for numeric temperature folders and opens each temperature's
        <code>.nxs</code> to detect which stages are already complete, so the generated script
        <strong>resumes from the first incomplete stage</strong>. A <code>*_parent.nxs</code> enables
        the full chain; without one, only <strong>load, link, max, find</strong> are generated.
      </p>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:140px;">Sample dir (USER_DIR)</label>
        <input type="text" id="scan_user_dir" style="flex:1;max-width:560px;"
               placeholder="/nfs/chess/id4baux/2026-2/PI/nxrefine/SAMPLE/">
        <button type="button" class="browse-btn" onclick="openDirPicker('scan_user_dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:140px;">Entries</label>
        <input type="text" id="scan_entries" value="f1 f2 f3" style="width:220px;">
        <span class="hint" style="margin:0;">space-separated entry names</span>
      </div>

      <div style="display:flex;gap:14px;align-items:center;flex-wrap:wrap;">
        <span class="field-label" style="margin:0;min-width:140px;">Detection</span>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="scan_mode_radio" value="exact" checked onchange="scanModeChanged()">
          &#128269; Exact (read each <code>.nxs</code>)
        </label>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="scan_mode_radio" value="file" onchange="scanModeChanged()">
          File-count (fast)
        </label>
        <span style="flex:1;"></span>
        <button type="button" class="btn btn-primary" style="padding:8px 20px;font-size:13px;"
                onclick="triggerScan()">&#128269; Scan Now</button>
      </div>
      <p class="hint" id="scan-mode-desc" style="margin:0;">
        <strong>Exact:</strong> opens each temperature's .nxs, detects completed stages, and the script resumes from the first incomplete stage.
      </p>

      <div id="autoscan-panel" class="scan-panel" style="margin-top:2px;"></div>

      <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;border-top:1px solid var(--border);padding-top:14px;">
        <span class="field-label" style="margin:0;min-width:140px;">Run on selected</span>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="scan_gen_mode" value="preview" checked> Generate the script only
        </label>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="scan_gen_mode" value="submit"> Generate and submit script
        </label>
        <span style="flex:1;"></span>
        <button type="button" class="btn btn-submit" style="padding:8px 22px;font-size:13px;"
                onclick="exactGenerate()">&#9654; Run selected temperatures</button>
      </div>
      <p class="hint" style="margin:0;">Tick the temperatures in the results above, pick a mode, then run. Uses the queue, email and script-folder settings from the <strong>Submit Job</strong> tab.</p>
    </div>
  </div>
<!-- ── Auto-scan results ────────────────────────────────────────────────── -->
  {% if autoscan_results %}
  <div class="card" style="border-color:rgba(56,189,248,.3);">
    <div class="card-header">
      <div class="card-title"><div class="dot"></div>
        Auto-Scan: {{ autoscan_results|length }} script{% if autoscan_results|length != 1 %}s{% endif %}
        {% if autoscan_action == 'submit' %}generated &amp; submitted{% else %}generated{% endif %}
      </div>
      <code style="font-size:11px;color:var(--text-muted);">{{ autoscan_sdir }}</code>
    </div>
    {% if autoscan_csv %}
    <div style="padding:8px 16px;font-size:12px;border-bottom:1px solid var(--border);">
      &#128202; CSV summary saved: <code style="color:#22c55e;">{{ autoscan_csv }}</code>
    </div>
    {% endif %}
    <table>
      <thead>
        <tr>
          <th>Temp</th>
          <th>Script file</th>
          {% if autoscan_action == 'submit' %}<th>Job result</th>{% endif %}
          <th>qsub command</th>
        </tr>
      </thead>
      <tbody>
        {% for r in autoscan_results %}
        <tr>
          <td><span class="chip chip-gold" style="font-size:12px;">{{ r.temp }}</span></td>
          <td style="font-family:var(--mono);font-size:11px;word-break:break-all;">
            {% if r.script_path %}{{ r.script_path }}{% else %}<span style="color:#ef4444;">save failed</span>{% endif %}
            {% if r.error %}<br><span style="color:#f59e0b;font-size:10px;">⚠ {{ r.error }}</span>{% endif %}
          </td>
          {% if autoscan_action == 'submit' %}
          <td>
            {% if r.job_ok %}
            <span class="job-ok">✓ {{ r.job_output }}</span>
            {% elif r.job_output %}
            <span class="job-fail">✗ {{ r.job_output }}</span>
            {% else %}
            <span style="color:var(--text-muted);">—</span>
            {% endif %}
          </td>
          {% endif %}
          <td style="font-family:var(--mono);font-size:11px;color:#38bdf8;word-break:break-all;">
            {% if r.qsub_cmd %}{{ r.qsub_cmd }}{% else %}—{% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% endif %}

  <!-- ── Raw scans missing (skipped) ──────────────────────────────────────────── -->
  {% if autoscan_raw_skipped %}
  <div class="card" style="border-color:rgba(251,191,36,.35);">
    <div class="card-header">
      <div class="card-title"><div class="dot" style="background:#fbbf24;"></div>
        Skipped — raw scans missing ({{ autoscan_raw_skipped|length }})
      </div>
      <code style="font-size:11px;color:var(--text-muted);">need {{ raw_required_scans }} scans, &ge;{{ raw_images_per_scan }} {{ raw_image_ext }} each</code>
    </div>
    <table>
      <thead>
        <tr><th>Temp</th><th>Raw folder</th><th>Scans found</th><th>Reason</th></tr>
      </thead>
      <tbody>
        {% for r in autoscan_raw_skipped %}
        <tr>
          <td><span class="chip chip-muted" style="font-size:12px;">{{ r.temp }}</span></td>
          <td style="font-family:var(--mono);font-size:11px;word-break:break-all;color:var(--text-muted);">{{ r.raw_dir }}</td>
          <td style="font-family:var(--mono);font-size:12px;">{{ r.n_scans }}</td>
          <td style="font-size:11px;color:#f59e0b;">{{ r.reason }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    <div style="padding:8px 16px;font-size:11px;color:var(--text-muted);">
      No script or CSV row was generated for these temperatures.
    </div>
  </div>
  {% endif %}

    </div><!-- /tab-scan -->

  <div class="tabpanel" id="tab-watch">
<!-- ── Live Watcher ─────────────────────────────────────────────────────── -->
  <div class="card" id="watcher-card">
    <div class="card-header">
      <div class="card-title"><div class="dot dot-green" id="watch-pulse-dot"></div>Live Watcher</div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span id="watch-last-update" style="font-size:11px;color:var(--text-muted);"></span>
        <button class="btn btn-clear" style="padding:4px 12px;font-size:11px;"
                onclick="watchToggle()" id="watch-toggle-btn">⏸ Pause</button>
        <button class="btn btn-clear" style="padding:4px 12px;font-size:11px;"
                onclick="watchRefresh()">↻ Refresh</button>
      </div>
    </div>

    <!-- Directory input -->
    <div class="watch-input-row" style="flex-wrap:wrap;">
      <label class="field-label" style="margin:0;white-space:nowrap;">Watch directory</label>
      <input type="text" id="watch-dir-input" style="flex:1;max-width:600px;"
             placeholder="/nfs/chess/id4baux/2026-2/PI/nxrefine/SAMPLE/"
             oninput="watchDirChanged()">
      <button type="button" class="browse-btn" id="watch-browse-btn" onclick="watchBrowse()">&#128193; Browse</button>
      <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;white-space:nowrap;">
        <input type="checkbox" id="watch-sync-chk" checked onchange="watchSyncToggle()">
        Same as Sample dir
      </label>
      <select id="watch-interval-sel" onchange="watchIntervalChanged()"
              style="padding:6px 10px;border:1px solid var(--border);border-radius:6px;
                     font-size:12px;background:var(--bg-card);color:var(--text);font-family:var(--sans);">
        <option value="5">Every 5s</option>
        <option value="10">Every 10s</option>
        <option value="30">Every 30s</option>
        <option value="60">Every 60s</option>
      </select>
    </div>

    <!-- Dashboard -->
    <div class="watch-grid" style="padding:14px;gap:14px;">
      <!-- File progress pane -->
      <div class="watch-pane">
        <div class="watch-pane-title">
          <span>📁 File Progress per Temperature</span>
          <span id="watch-temp-summary" style="margin-left:auto;font-size:10px;font-weight:400;"></span>
        </div>
        <div class="watch-body" id="watch-temp-body">
          <div style="color:var(--text-muted);font-size:12px;padding:20px;text-align:center;">
            Enter a directory above to start watching.
          </div>
        </div>
      </div>
      <!-- qstat pane -->
      <div class="watch-pane">
        <div class="watch-pane-title">
          <span>🖥 SGE Job Queue (qstat)</span>
          <span id="watch-qstat-count" style="margin-left:auto;font-size:10px;font-weight:400;"></span>
        </div>
        <div class="watch-body" id="watch-qstat-body">
          <div style="color:var(--text-muted);font-size:12px;padding:20px;text-align:center;">
            Waiting for first refresh…
          </div>
        </div>
      </div>
    </div>

    <div class="watch-bar">
      <div class="pulse paused" id="watch-pulse"></div>
      <span id="watch-status-text">Paused — enter a directory to start.</span>
    </div>
  </div>

    </div><!-- /tab-watch -->

  <div class="tabpanel" id="tab-auto">
<!-- ── Auto-Submit (parent-aware, separate from Auto-Watch) ─────────────────── -->
  <div class="card" id="autosubmit-card" style="border-color:rgba(16,185,129,.35);">
    <div class="card-header">
      <div class="card-title"><div class="dot" style="background:#10b981;" id="as-pulse-dot"></div>Auto-Submit (parent-aware)</div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span id="as-last-update" style="font-size:11px;color:var(--text-muted);"></span>
        <button class="btn btn-clear" style="padding:4px 12px;font-size:11px;" onclick="asReset()">↺ Reset handled</button>
      </div>
    </div>

    <div style="padding:12px 16px;display:flex;flex-direction:column;gap:10px;">
      <p class="hint" style="margin:0;">
        A separate auto-submitter. For every temperature <em>except the parent temp below</em>
        it auto-generates and (optionally) submits a script: <strong>no parent file yet →
        load, link, max, find</strong>; <strong>parent file present → full chain
        load, link, copy, max, find, refine, transform, combine</strong>. The
        <strong>parent temperature is skipped</strong> because that folder is the parent,
        not a child dataset. Each temperature is handled once per stage.
      </p>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">Sample dir (USER_DIR)</label>
        <input type="text" id="as-user-dir" style="flex:1;max-width:560px;"
               placeholder="/nfs/chess/id4baux/2026-2/PI/nxrefine/SAMPLE/">
        <button type="button" class="browse-btn" onclick="openDirPicker('as-user-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">chmod folder</label>
        <input type="text" id="as-chmod-dir" style="flex:1;max-width:560px;"
               placeholder="/nfs/chess/id4baux/2026-2/PI/">
        <button type="button" class="browse-btn" onclick="openDirPicker('as-chmod-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">Script folder</label>
        <input type="text" id="as-script-dir" style="flex:1;max-width:560px;"
               placeholder="(default: PI-level /scripts, outside nxrefine)">
        <button type="button" class="browse-btn" onclick="openDirPicker('as-script-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">Parent temp (skip)</label>
        <input type="number" id="as-parent-temp" value="300" style="width:110px;">
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;white-space:nowrap;">
          <input type="checkbox" id="as-auto-parent" checked> Auto-detect from <code>*_parent.nxs</code>
        </label>
        <span class="hint" id="as-parent-hint" style="margin:0;">Auto-detect reads the parent file; the number above is the fallback until it exists.</span>
      </div>

      <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;">
        <span class="field-label" style="margin:0;">Action</span>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="as_mode" value="generate"> chmod + generate script
        </label>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="as_mode" value="submit" checked> chmod + generate + submit
        </label>
        <span style="flex:1;"></span>
        <select id="as-interval" style="padding:6px 10px;border:1px solid var(--border);border-radius:6px;font-size:12px;background:var(--bg-card);color:var(--text);font-family:var(--sans);">
          <option value="10">Every 10s</option>
          <option value="30" selected>Every 30s</option>
          <option value="60">Every 60s</option>
          <option value="120">Every 2 min</option>
        </select>
        <button type="button" class="btn btn-primary" id="as-toggle-btn" style="padding:7px 18px;font-size:13px;" onclick="asToggle()">▶ Start Auto-Submit</button>
      </div>
    </div>

    <div class="watch-bar">
      <div class="pulse paused" id="as-pulse"></div>
      <span id="as-status-text">Stopped. Set a Sample directory and press Start.</span>
    </div>

    <!-- activity log -->
    <div class="watch-body" id="as-log-body" style="max-height:280px;border-top:1px solid var(--border);">
      <div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">No activity yet.</div>
    </div>
  </div>

  <!-- ── Auto-Watch (raw6M trigger) ───────────────────────────────────────────── -->
  <div class="card" id="autowatch-card" style="border-color:rgba(168,85,247,.35);">
    <div class="card-header">
      <div class="card-title"><div class="dot" style="background:#a855f7;" id="aw-pulse-dot"></div>Auto-Watch (raw6M trigger)</div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span id="aw-last-update" style="font-size:11px;color:var(--text-muted);"></span>
        <button class="btn btn-clear" style="padding:4px 12px;font-size:11px;" onclick="awReset()">↺ Reset handled</button>
      </div>
    </div>

    <div style="padding:12px 16px;display:flex;flex-direction:column;gap:10px;">
      <p class="hint" style="margin:0;">
        Watches raw6M for each temperature. As soon as a temperature has
        <strong>{{ raw_required_scans }} valid scan folders</strong> (&ge;{{ raw_images_per_scan }} {{ raw_image_ext }} each),
        it runs <code>chmod -R 777</code> on the folder below, auto-generates the script
        (no parent → <strong>load, link, max, find</strong>; parent present → full chain
        load…transform, combine), and optionally submits it. Each temperature is handled once per stage.
      </p>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">Sample dir (USER_DIR)</label>
        <input type="text" id="aw-user-dir" style="flex:1;max-width:560px;"
               placeholder="/nfs/chess/id4baux/2026-2/PI/nxrefine/SAMPLE/">
        <button type="button" class="browse-btn" onclick="openDirPicker('aw-user-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">chmod folder</label>
        <input type="text" id="aw-chmod-dir" style="flex:1;max-width:560px;"
               value="/nfs/chess/id4baux/2026-2/sarker-0000-a/"
               placeholder="/nfs/chess/id4baux/2026-2/PI/">
        <button type="button" class="browse-btn" onclick="openDirPicker('aw-chmod-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <label class="field-label" style="margin:0;white-space:nowrap;min-width:120px;">Script folder</label>
        <input type="text" id="aw-script-dir" style="flex:1;max-width:560px;"
               placeholder="(default: PI-level /scripts, outside nxrefine)">
        <button type="button" class="browse-btn" onclick="openDirPicker('aw-script-dir')">&#128193; Browse</button>
      </div>

      <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;">
        <span class="field-label" style="margin:0;">Action</span>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="aw_mode" value="generate" checked> chmod + generate script
        </label>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;">
          <input type="radio" name="aw_mode" value="submit"> chmod + generate + submit
        </label>
        <span style="flex:1;"></span>
        <select id="aw-interval" style="padding:6px 10px;border:1px solid var(--border);border-radius:6px;font-size:12px;background:var(--bg-card);color:var(--text);font-family:var(--sans);">
          <option value="10">Every 10s</option>
          <option value="30" selected>Every 30s</option>
          <option value="60">Every 60s</option>
          <option value="120">Every 2 min</option>
        </select>
        <button type="button" class="btn btn-primary" id="aw-toggle-btn" style="padding:7px 18px;font-size:13px;" onclick="awToggle()">▶ Start Auto-Watch</button>
      </div>
    </div>

    <div class="watch-bar">
      <div class="pulse paused" id="aw-pulse"></div>
      <span id="aw-status-text">Stopped. Set a Sample directory and press Start.</span>
    </div>

    <!-- activity log -->
    <div class="watch-body" id="aw-log-body" style="max-height:280px;border-top:1px solid var(--border);">
      <div style="color:var(--text-muted);font-size:12px;padding:14px;text-align:center;">No activity yet.</div>
    </div>
  </div>

    </div><!-- /tab-auto -->

  <div class="tabpanel" id="tab-health">
<!-- ── Jobs needing attention (qstat: error / held / suspended + gone-no-output) ── -->
  <div class="card" id="jobhealth-card" style="border-color:rgba(220,38,38,.35);">
    <div class="card-header">
      <div class="card-title"><div class="dot" style="background:#dc2626;" id="jh-dot"></div>Jobs needing attention</div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span id="jh-last-update" style="font-size:11px;color:var(--text-muted);"></span>
        <label style="display:flex;align-items:center;gap:6px;font-size:11px;cursor:pointer;color:var(--text-muted);">
          <input type="checkbox" id="jh-autorefresh" checked> Auto-refresh with pollers
        </label>
        <button class="btn btn-clear" style="padding:4px 12px;font-size:11px;" onclick="jhRefresh()">↻ Refresh</button>
      </div>
    </div>

    <div style="padding:12px 16px;display:flex;flex-direction:column;gap:10px;">
      <p class="hint" style="margin:0;">
        Only <strong>your nxrefine jobs submitted from this app</strong> (any panel) —
        other people's cluster jobs are never shown. Flags the ones
        <strong>not completing properly</strong>: SGE <strong>error</strong> (Eqw),
        <strong>held</strong> (hqw) or <strong>suspended</strong> states, plus jobs that
        <strong>left the queue with no output</strong>. Everything the app knows about
        each one is shown below.
      </p>
      <div id="jh-status" style="font-size:12px;color:var(--text-muted);">Not checked yet — press Refresh.</div>
      <div id="jh-table-wrap" style="overflow-x:auto;"></div>
    </div>
  </div>

  </div><!-- /tab-health -->

</div><!-- /page -->

<!-- ── Directory picker modal ──────────────────────────────────────────────── -->
<div class="modal-overlay" id="dir-modal">
  <div class="modal-box">
    <div class="modal-header">
      <span class="modal-title">&#128193; Choose Directory</span>
      <button class="modal-close" onclick="closeDirPicker()">&#215;</button>
    </div>
    <div class="modal-crumbs" id="dp-crumbs"></div>
    <div class="modal-body">
      <div id="dp-loading" style="padding:30px;text-align:center;color:var(--text-muted);font-size:13px;">
        Loading&hellip;
      </div>
      <div id="dp-list"></div>
    </div>
    <div class="modal-footer">
      <span class="modal-cur" id="dp-cur"></span>
      <button type="button" class="btn btn-clear" style="padding:6px 14px;font-size:12px;" onclick="closeDirPicker()">Cancel</button>
      <button type="button" class="btn btn-primary" style="padding:6px 16px;font-size:12px;" onclick="_dpSelect()">
        &#10003; Select This Folder
      </button>
    </div>
  </div>
</div>

</body>
</html>
"""

# ── Helpers ───────────────────────────────────────────────────────────────────
import re as _re

def _script_name(user_dir, temps_str):
    """Build a human-readable script filename, e.g. wilson-CeCd3P3_b3_s1-91.sh"""
    ud = user_dir.rstrip("/")
    # Strip trailing numeric temperature directory if the full path was entered
    # e.g. .../CeCd3P3_b3_s1/91/ → use .../CeCd3P3_b3_s1 as the base
    _last = os.path.basename(ud)
    if _last.lstrip('-').isdigit() and _last:
        ud = os.path.dirname(ud).rstrip("/")
    # PI short name: first component below ROOT, strip trailing -digits, e.g. wilson-4797-a → wilson
    try:
        rel = ud[len(ROOT):].lstrip("/") if ud.startswith(ROOT) else ud.lstrip("/")
        pi_folder = rel.split("/")[0] if rel else ""
        pi_short  = _re.split(r'-\d', pi_folder)[0] if pi_folder else "job"
    except Exception:
        pi_short = "job"
    # Sample name = last path component of USER_DIR
    sample = os.path.basename(ud) if ud else "sample"
    # Compact temperature string
    parts = temps_str.strip().split()
    if len(parts) == 1:
        tc = parts[0]
    elif len(parts) <= 6:
        tc = "_".join(parts)
    else:
        tc = f"{parts[0]}-{parts[-1]}"
    base = _re.sub(r'[^\w\-]', '_', f"{pi_short}-{sample}-{tc}")
    # Avoid overwriting: append _2, _3 … if the file already exists
    return base


def _build_script(user_dir, temps_str, entries, steps, threshold, chmod_path,
                  notify_email="", notify_end="0", notify_abort="0", notify_begin="0"):
    ent = entries.strip() or "f1 f2 f3"
    ud  = user_dir.rstrip("/") + "/"
    # SGE email directives
    email_lines = []
    if notify_email.strip():
        email_lines.append(f"#$ -M {notify_email.strip()}")
        flags = ""
        if notify_begin == "1": flags += "b"
        if notify_end   == "1": flags += "e"
        if notify_abort == "1": flags += "a"
        if flags:
            email_lines.append(f"#$ -m {flags}")
    L = [
        "#!/bin/bash",
    ] + email_lines + [
        "LOCAL_DIR=${TMPDIR}",
        "echo running on host: `hostname`",
        'echo `date` "USER ${USER} JOB_ID ${JOB_ID}"',
        "source /nfs/chess/id4baux/nxserver/nxsetup.sh",
        "",
        "function nxrefine_process_temp () {",
        "    echo ${USER_DIR}${TEMP}",
    ]
    base = "    nxreduce --directory ${USER_DIR}${TEMP}"
    if "load"       in steps: L.append(f"{base} --entries {ent} --load --overwrite")
    # granular stages (used by auto-scan / exact resume)
    if "link"       in steps: L.append(f"{base} --entries {ent} --link --overwrite")
    if "copy"       in steps: L.append(f"{base} --entries {ent} --copy --overwrite")
    if "max"        in steps: L.append(f"{base} --entries {ent} --max --overwrite")
    # combined stages (used by manual UI presets) — kept for backward compatibility
    if "link_copy"  in steps: L.append(f"{base} --entries {ent} --link --copy --max --overwrite")
    if "link_only"  in steps: L.append(f"{base} --entries {ent} --link --max --overwrite")
    if "link_ow"    in steps: L.append(f"{base} --entries {ent} --link --overwrite")
    if "copy_only"  in steps: L.append(f"{base} --entries {ent} --copy --overwrite")
    if "find"       in steps: L.append(f"    nxfind --directory ${{USER_DIR}}${{TEMP}} --entries {ent} -t {threshold} --overwrite")
    if "refine"     in steps: L.append(f"{base} --entries {ent} --refine --overwrite")
    # transform and combine run together in a single command (emit once if either present)
    if ("transform" in steps) or ("combine" in steps):
        L.append(f"{base} --entries {ent} --transform --combine --regular --overwrite")
    if "pdf"        in steps: L.append(f"    nxreduce --directory ${{USER_DIR}}${{TEMP}} --pdf --regular")
    L += [
        "}",
        "",
        f"USER_DIR='{ud}'",
        f"for TEMP in {temps_str};",
        "do",
        "    chmod -R 777 ${USER_DIR}${TEMP}",
        "    nxrefine_process_temp $TEMP",
        "done",
    ]
    if chmod_path and chmod_path.strip():
        L.append(f"chmod -R 777 {chmod_path.strip()}")
    return "\n".join(L) + "\n"


def _default_script_dir(user_dir):
    """Return the PI-proposal-level scripts folder, OUTSIDE nxrefine.
    e.g. /nfs/chess/id4baux/2026-2/gomez-al-4850-a/nxrefine/FeGe/SAMPLE/
      →  /nfs/chess/id4baux/2026-2/gomez-al-4850-a/scripts
    Works regardless of dataset depth by anchoring on the 'nxrefine' component."""
    ud = user_dir.rstrip("/")
    parts = ud.split("/")
    if "nxrefine" in parts:
        idx = parts.index("nxrefine")           # PI folder is the parent of nxrefine
        pi_level = "/".join(parts[:idx])
    else:
        # fallback: two levels up from the dataset folder
        pi_level = os.path.dirname(os.path.dirname(ud))
    return os.path.join(pi_level, "scripts") if pi_level else "/tmp/nxreduce_scripts"


def _csv_basename(user_dir):
    """Build the CSV base name from the last two components of USER_DIR.
    e.g. /…/nxrefine/FeGe/FeGe_MY_noBuffer/ → 'FeGe_FeGe_MY_noBuffer'."""
    ud = user_dir.rstrip("/")
    parts = [p for p in ud.split("/") if p]
    if len(parts) >= 2:
        return f"{parts[-2]}_{parts[-1]}"
    return parts[-1] if parts else "autoscan"


def _write_autoscan_csv(results, sdir, csv_base):
    """Write a CSV summarizing generated scripts, OVERWRITING any existing CSV of
    the same name. Returns (path, warn_or_none).
    Columns: Temp, Script file, qsub command."""
    import csv as _csv
    fname = csv_base + ".csv"
    candidate = os.path.join(sdir, fname)
    try:
        os.makedirs(sdir, exist_ok=True)
        with open(candidate, "w", newline="") as f:
            w = _csv.writer(f)
            w.writerow(["Temp", "Script file", "qsub command"])
            for r in results:
                w.writerow([
                    r.get("temp", ""),
                    os.path.basename(r["script_path"]) if r.get("script_path") else "",
                    r.get("qsub_cmd", "") or "",
                ])
        try:
            os.chmod(candidate, 0o666)
        except Exception:
            pass
        return candidate, None
    except Exception as ex:
        return None, f"CSV write failed: {ex}"


def _save_one_script(script_text, sdir, base_name):
    """Save a single script to sdir, OVERWRITING any existing file of the same
    name (so re-running generate updates in place rather than creating copies).
    Returns (path, warn_or_none)."""
    sname     = base_name + ".sh"
    candidate = os.path.join(sdir, sname)
    warn = None
    try:
        os.makedirs(sdir, exist_ok=True)
        with open(candidate, "w") as f:
            f.write(script_text)
        os.chmod(candidate, 0o755)
    except Exception as _io_err:
        _fallback = "/tmp/nxreduce_scripts"
        try:
            os.makedirs(_fallback, exist_ok=True)
            candidate = os.path.join(_fallback, sname)
            with open(candidate, "w") as f:
                f.write(script_text)
            os.chmod(candidate, 0o755)
            warn = f"Could not write to {sdir!r} ({_io_err}). Saved to fallback: {candidate}"
        except Exception as _fb_err:
            return None, f"Script save failed: {_io_err} | fallback also failed: {_fb_err}"
    return candidate, warn


def _submit_autoscan(fv, user_dir, action):
    """Handle auto-scan mode: scan USER_DIR, build one script per temp, save (and optionally qsub) each."""
    errors = []
    ud = user_dir.rstrip("/") + "/"

    # ── check parent file (no longer required — switches mode if absent) ───────
    try:
        parent_files = [f for f in os.listdir(user_dir)
                        if f.endswith("_parent.nxs") and os.path.isfile(os.path.join(user_dir, f))]
    except Exception as e:
        return _render(errors=[f"Cannot read USER_DIR: {e}"],
                       script_text=None, script_path=None, qsub_cmd=None,
                       job_output=None, job_stderr=None, job_ok=False,
                       autoscan_results=None, **fv)
    has_parent = bool(parent_files)

    # ── scan for temps needing processing (any folder with <10 files) ─────────
    to_process = []          # list of temp names
    temp_counts = {}         # temp -> file count
    try:
        for name in sorted(os.listdir(user_dir), key=lambda x: int(x) if x.lstrip('-').isdigit() else 0):
            if not name.lstrip('-').isdigit():
                continue
            full = os.path.join(user_dir, name)
            if not os.path.isdir(full):
                continue
            try:
                n = len([f for f in os.listdir(full) if os.path.isfile(os.path.join(full, f))])
            except PermissionError:
                continue
            if n < 10:                 # 0–9 files → needs processing; >=10 → done, skip
                to_process.append(name)
                temp_counts[name] = n
    except Exception as e:
        return _render(errors=[f"Scan failed: {e}"],
                       script_text=None, script_path=None, qsub_cmd=None,
                       job_output=None, job_stderr=None, job_ok=False,
                       autoscan_results=None, **fv)

    if not to_process:
        return _render(errors=["Auto-scan found no temperature folders needing processing (all have 10+ files)."],
                       script_text=None, script_path=None, qsub_cmd=None,
                       job_output=None, job_stderr=None, job_ok=False,
                       autoscan_results=None, **fv)

    # ── filter to only the temps the user ticked (if a selection was sent) ─────
    selected_raw = (fv.get("autoscan_selected") or "").strip()
    if selected_raw:
        selected = set(t.strip() for t in selected_raw.split(",") if t.strip())
        to_process = [t for t in to_process if t in selected]
        if not to_process:
            return _render(errors=["No temperatures selected. Tick at least one temperature to generate scripts."],
                           script_text=None, script_path=None, qsub_cmd=None,
                           job_output=None, job_stderr=None, job_ok=False,
                           autoscan_results=None, **fv)

    # ── raw-data scan check: each temp must have 3 scan folders with 3651 .cbf ─
    raw_skipped = []   # [{temp, reason, raw_dir}]
    kept = []
    for temp in to_process:
        ok, info = _check_raw_scans(user_dir, temp)
        if ok:
            kept.append(temp)
        else:
            raw_skipped.append({"temp": temp, "reason": info["reason"],
                                "raw_dir": info["raw_dir"],
                                "n_scans": info["n_scans"]})
    to_process = kept
    if not to_process:
        msg = "All selected temperatures are missing raw scans (need {} scan folders, ≥{} .cbf each).".format(RAW_REQUIRED_SCANS, RAW_MIN_IMAGES_PER_SCAN)
        return _render(errors=[msg],
                       script_text=None, script_path=None, qsub_cmd=None,
                       job_output=None, job_stderr=None, job_ok=False,
                       autoscan_results=None, autoscan_raw_skipped=raw_skipped, **fv)

    # ── steps for auto-scan ────────────────────────────────────────────────────
    # Granular pipeline: load → link → copy → max → find → refine → transform.
    full_steps    = ["load", "link", "copy", "max", "find", "refine", "transform", "combine"]
    no_load_steps = ["link", "copy", "max", "find", "refine", "transform", "combine"]
    # No-parent chain: only load, link, max, find (no copy/refine/transform).
    no_parent_steps = list(NO_PARENT_STAGES)
    # Which stage set applies given parent availability:
    relevant_stages = full_steps if has_parent else no_parent_steps
    threshold = fv["find_threshold"].strip() or "80000"
    chmod_path = fv["chmod_path"].strip()
    ent       = (fv.get("entries") or "f1 f2 f3").strip()
    exact     = fv.get("autoscan_exact", "0") == "1"

    # In exact mode, compute remaining steps per temp by inspecting its .nxs.
    # Only stages in `relevant_stages` are considered (so no-parent never emits
    # copy/refine/transform).
    exact_steps = {}   # temp -> [remaining step keys]
    if exact:
        for temp in to_process:
            nxs = _temp_nxs_path(user_dir, temp)
            done, _note = _detect_completed_stages(nxs, ent)
            rem = _remaining_steps(done, relevant_stages)
            exact_steps[temp] = rem
        # drop temps that have nothing left to do (exact mode only)
        to_process = [t for t in to_process if exact_steps.get(t)]
        if not to_process:
            return _render(errors=["Exact auto-scan: all selected temperatures are already complete."],
                           script_text=None, script_path=None, qsub_cmd=None,
                           job_output=None, job_stderr=None, job_ok=False,
                           autoscan_results=None, **fv)

    # ── script output dir ──────────────────────────────────────────────────────
    sdir = fv["script_dir"].strip()
    if not sdir:
        sdir = _default_script_dir(ud)

    # ── queue settings ─────────────────────────────────────────────────────────
    q     = fv["q_queue"].strip()  or "all.q"
    mem   = fv["q_mem"].strip()    or "300G"
    pe    = fv["q_pe"].strip()     or "sge_pe"
    cores = fv["q_cores"].strip()  or "12"

    # ── build, save, (optionally submit) one script per temp ──────────────────
    results = []   # list of dicts: {temp, script_path, qsub_cmd, job_output, job_ok, error}
    for temp in to_process:
        if exact:
            steps = exact_steps.get(temp) or relevant_stages
        elif not has_parent:
            # No parent, file-count mode → fixed load, link, max, find chain
            steps = no_parent_steps
        else:
            n = temp_counts.get(temp, 0)
            steps = no_load_steps if n == 3 else full_steps
        txt = _build_script(
            ud, temp, ent, steps, threshold, chmod_path,
            notify_email=fv.get("notify_email", ""),
            notify_end=fv.get("notify_end", "0"),
            notify_abort=fv.get("notify_abort", "0"),
            notify_begin=fv.get("notify_begin", "0"),
        )
        base  = _script_name(ud, temp)
        spath, warn = _save_one_script(txt, sdir, base)
        if spath is None:
            results.append({"temp": temp, "script_path": None, "qsub_cmd": None,
                             "job_output": None, "job_ok": False, "error": warn})
            continue

        qcmd = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {spath}"
        entry = {"temp": temp, "script_path": spath, "script_text": txt,
                 "qsub_cmd": qcmd, "job_output": None, "job_ok": False,
                 "error": warn}  # warn is non-fatal

        if action == "submit":
            try:
                res = _run_qsub(qcmd, _ssh_cfg_from_form(request.form), timeout=30)
                entry["job_output"] = res.stdout.strip() or "(no stdout)"
                entry["job_stderr"] = res.stderr.strip()
                entry["job_ok"]     = res.returncode == 0
                if entry["job_ok"]:
                    _record_submitted(_extract_job_id(res.stdout))
            except FileNotFoundError:
                entry["job_output"] = ("qsub not found — is the SGE environment loaded? "
                                       "Run: source /nfs/chess/id4baux/nxserver/nxsetup.sh")
            except Exception as ex:
                entry["job_output"] = str(ex)

            # record in history
            step_labels = {"load": "Load", "link": "Link", "copy": "Copy", "max": "Max",
                           "link_copy": "Link+Copy", "find": "Find",
                           "refine": "Refine", "transform": "Transform", "combine": "Combine"}
            with _history_lock:
                _job_history.append({
                    "time":        datetime.datetime.now().strftime("%H:%M:%S"),
                    "script_name": os.path.basename(spath),
                    "temps":       temp,
                    "steps":       ", ".join(step_labels[s] for s in steps),
                    "ok":          entry["job_ok"],
                    "job_id":      (entry["job_output"] or "").split("\n")[0][:60] if entry["job_ok"] else "",
                    "detail":      "" if entry["job_ok"] else ((entry.get("job_stderr") or entry.get("job_output") or entry.get("error") or "").strip()[:300]),
                })

        results.append(entry)

    # ── write CSV summary alongside the scripts ────────────────────────────────
    csv_path, csv_warn = (None, None)
    if any(r.get("script_path") for r in results):
        csv_base = _csv_basename(user_dir)
        csv_path, csv_warn = _write_autoscan_csv(results, sdir, csv_base)
    if csv_warn:
        errors = list(errors) + [csv_warn]

    return _render(errors=errors, script_text=None, script_path=None, qsub_cmd=None,
                   job_output=None, job_stderr=None, job_ok=False,
                   autoscan_results=results, autoscan_sdir=sdir, autoscan_action=action,
                   autoscan_csv=csv_path, autoscan_raw_skipped=raw_skipped,
                   **fv)


def _do_chmod_777(pc_raw):
    """Run `chmod -R 777` on a path, confined to /nfs/chess/ for safety.
    Returns (ok: bool, message: str)."""
    pc_raw = (pc_raw or "").strip()
    if not pc_raw:
        return False, "Pre-submission chmod is enabled but no path was provided."
    pc_path = os.path.normpath(pc_raw)
    if not pc_path.startswith("/nfs/chess/"):
        return False, f"Refused to chmod {pc_path!r}: path must be under /nfs/chess/ for safety."
    try:
        _pc = subprocess.run(["chmod", "-R", "777", pc_path],
                             capture_output=True, text=True, timeout=120)
        if _pc.returncode == 0:
            return True, f"chmod -R 777 {pc_path} (all files updated)"
        # Non-zero exit is usually because some files are owned by a different
        # user: chmod can only change permissions on files you own, so those get
        # skipped with "Operation not permitted". This is the SAME result you get
        # running the command manually in the terminal. Treat a permission-only
        # outcome as a non-fatal warning so the job can still be submitted.
        err = (_pc.stderr or _pc.stdout or "").strip()
        err_lines = [ln for ln in err.splitlines() if ln.strip()]
        perm_lines = [ln for ln in err_lines if "Operation not permitted" in ln]
        if err_lines and len(perm_lines) == len(err_lines):
            return True, (f"chmod -R 777 {pc_path} completed with warnings: "
                          f"{len(perm_lines)} file(s) skipped because they are owned by "
                          f"another user (\"Operation not permitted\"). This is the same "
                          f"result as running the command yourself in the terminal — every "
                          f"file you own was updated, so you can proceed.")
        return False, (f"chmod -R 777 {pc_path} failed (exit {_pc.returncode}): "
                       f"{err or 'no output'}")
    except Exception as _pc_ex:
        return False, f"chmod -R 777 {pc_path} errored: {_pc_ex}"


def _parse_temps(mode, single, start, end, step, lst):
    try:
        if mode == "single":
            return str(int(float(single or "0"))), None
        if mode == "range":
            s, e, st = int(float(start or "0")), int(float(end or "0")), int(float(step or "1"))
            if st == 0: return None, "Step cannot be zero."
            vals = list(range(s, e + (1 if st > 0 else -1), st))
            if not vals: return None, "Range produces no values."
            return " ".join(str(v) for v in vals), None
        # list mode
        raw  = (lst or "").replace(",", " ").split()
        vals = [int(float(v)) for v in raw if v.strip()]
        if not vals: return None, "Temperature list is empty."
        return " ".join(str(v) for v in vals), None
    except Exception as ex:
        return None, f"Temperature parse error: {ex}"


# ── Routes ────────────────────────────────────────────────────────────────────
def _render(**ctx):
    with _history_lock:
        ctx["history"] = list(_job_history)
    ctx.setdefault("root_path", ROOT)
    ctx.setdefault("default_queue", "all.q")
    ctx.setdefault("autoscan_results", None)
    ctx.setdefault("autoscan_sdir", "")
    ctx.setdefault("autoscan_action", "preview")
    ctx.setdefault("autoscan_csv", None)
    ctx.setdefault("autoscan_raw_skipped", None)
    ctx.setdefault("raw_required_scans", RAW_REQUIRED_SCANS)
    ctx.setdefault("raw_images_per_scan", RAW_MIN_IMAGES_PER_SCAN)
    ctx.setdefault("raw_image_ext", RAW_IMAGE_EXT)
    ctx.setdefault("pre_chmod_output", None)
    ctx.setdefault("pre_chmod_ok", False)
    ctx.setdefault("pre_chmod_enable", "0")
    ctx.setdefault("pre_chmod_path", "")
    ctx.setdefault("remote_enable", "0")
    ctx.setdefault("remote_host", "lnx201.classe.cornell.edu")
    ctx.setdefault("remote_user", "ss3428")
    ctx.setdefault("remote_key", "~/.ssh/id_rsa")
    return render_template_string(PAGE, **ctx)


@app.route("/logo")
def serve_logo():
    """Serve the CHESS logo from the NFS path."""
    if os.path.isfile(CHESS_LOGO):
        return send_file(CHESS_LOGO, mimetype="image/png")
    # Return a transparent 1×1 PNG so the img onerror fallback fires
    import base64
    _blank = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
    return Response(_blank, mimetype="image/png")


# ─────────────────────────────────────────────────────────────────────────────
# Exact stage detection — reads the temperature's .nxs with h5py to determine
# which processing stages are already complete, so scripts can resume.
#
# Pipeline stage order (each maps to a script step key):
#   load → link → copy → max → find → refine → transform
#
# CONSERVATIVE POLICY:
#   A stage is reported "done" ONLY when a rock-solid, stage-specific signal is
#   present in EVERY entry. Anything ambiguous → treated as NOT done, so the
#   stage is re-run (harmless because every command uses --overwrite).
#   Markers must NOT match groups that exist in a freshly-loaded file
#   (e.g. data, instrument, sample) — those caused false positives.
#
#   Empty marker list ([]) = "no reliable signal known" = ALWAYS re-run.
#   Fill these in once the real NeXus paths are confirmed (h5ls of a file at a
#   known stage), and detection becomes precise.
# ─────────────────────────────────────────────────────────────────────────────
STAGE_ORDER = ["load", "link", "copy", "max", "find", "refine", "transform", "combine"]

# When no *_parent.nxs is available, only these stages can run.
NO_PARENT_STAGES = ["load", "link", "max", "find"]

# Stage-specific completion markers (paths *within* an entry group, e.g. f1/...).
# A stage is done for an entry only if ANY listed path exists AND the path is
# specific to that stage's output. Empty = always re-run (safe default).
# nxrefine records each completed stage as an NXprocess group named
# ``nx<stage>`` inside the relevant entry (e.g. f1/nxfind, f1/nxrefine,
# f1/nxtransform, f1/nxcopy) and, for the whole-file combine step, as
# entry/nxcombine at the file root. The presence of that process group is the
# authoritative "this stage finished" marker — it is written only on success
# and its name is unique, so it cannot collide with generic datasets the way
# the old 'data'/'transform/data' markers did (those matched 'data' anywhere
# and made every stage look done). 'load' keeps the real data/data dataset as
# its signal since there is no nxload process record.
STAGE_MARKERS = {
    "load":      ["data/data"],      # raw data imported into the entry
    "link":      ["nxlink"],         # NXprocess record written by nxlink
    "copy":      ["nxcopy"],         # NXprocess record written by nxcopy
    "max":       ["nxmax"],          # NXprocess record written by nxmax
    "find":      ["nxfind"],         # NXprocess record written by nxfind
    "refine":    ["nxrefine"],       # NXprocess record written by nxrefine
    "transform": ["nxtransform"],    # NXprocess record written by nxtransform
    "combine":   ["nxcombine"],      # file-level NXprocess record (see below)
}

# combine is a whole-file step: its nxcombine record lives under the root
# 'entry' group, not inside each f1/f2/f3 entry. Detection searches the file
# root for these stages instead of requiring the marker in every entry.
FILE_LEVEL_STAGES = {"combine"}

# The nx<stage> markers are GROUPS (NXprocess records), so a non-empty group
# must count as 'done'. 'load' uses a dataset (data/data) and is unaffected.
STAGE_MARKERS_GROUP_OK = {"load", "link", "copy", "max",
                          "find", "refine", "transform", "combine"}


def _obj_is_complete(obj, group_ok):
    """A marker object counts as 'done' if it is a non-empty dataset, or a
    non-empty group when group_ok is True."""
    try:
        import h5py
    except Exception:
        return False
    try:
        if isinstance(obj, h5py.Group):
            return len(obj.keys()) > 0 if group_ok else False
        shape = getattr(obj, "shape", None)
        if shape is None:
            return True
        return all(d != 0 for d in shape) if len(shape) else True
    except Exception:
        return False


def _h5_find_by_name(grp, name, group_ok, max_depth=4):
    """Recursively search `grp` (up to max_depth) for any child whose basename
    equals `name` and that passes _obj_is_complete(). Robust to the marker
    living a level deeper/shallower than a hard-coded path."""
    try:
        import h5py
    except Exception:
        return False
    stack = [(grp, 0)]
    while stack:
        node, depth = stack.pop()
        try:
            for k in node.keys():
                try:
                    child = node[k]
                except Exception:
                    continue
                if k == name and _obj_is_complete(child, group_ok):
                    return True
                if isinstance(child, h5py.Group) and depth < max_depth:
                    stack.append((child, depth + 1))
        except Exception:
            continue
    return False


def _h5_marker_present(grp, path, group_ok):
    """True if a stage marker is present under `grp`.

    A marker may be given either as an explicit relative path (contains '/'),
    which is matched directly, or as a bare object name, which is searched for
    recursively anywhere under the entry. Either way the matched object must be
    a non-empty dataset (or a non-empty group when group_ok is True)."""
    try:
        import h5py
    except Exception:
        return False
    # explicit path first
    try:
        if path in grp and _obj_is_complete(grp[path], group_ok):
            return True
    except Exception:
        pass
    # recursive name fallback — ONLY for uniquely-named NXprocess records
    # (nx<stage>). Generic names like 'data' must never fall back recursively,
    # or they would match somewhere in almost every file and report false
    # "done" for later stages (this was the everything-looks-done bug).
    name = path.split("/")[-1]
    if name.startswith("nx"):
        return _h5_find_by_name(grp, name, group_ok)
    return False


def _detect_completed_stages(nxs_path, entries):
    """Caching wrapper around stage detection. Re-reads a .nxs only when its
    mtime/size changes (or the cache entry expires), so the same file isn't
    parsed repeatedly within a request cycle or watcher refresh."""
    if not nxs_path:
        return {s: False for s in STAGE_ORDER}, "no-nxs-file"
    try:
        st = os.stat(nxs_path)
        sig = (st.st_mtime_ns, st.st_size, entries)
    except OSError:
        return {s: False for s in STAGE_ORDER}, "no-nxs-file"

    now = time.time()
    key = nxs_path
    with _cache_lock:
        hit = _nxs_cache.get(key)
        if hit and hit[0] > now and hit[1] == sig:
            return hit[2]

    result = _detect_completed_stages_uncached(nxs_path, entries)
    with _cache_lock:
        _nxs_cache[key] = (now + _CACHE_TTL, sig, result)
    return result


def _detect_completed_stages_uncached(nxs_path, entries):
    """Return dict {stage: bool} of which stages appear complete in the .nxs file.

    Conservative: a stage is 'done' only if a stage-specific marker is present
    for EVERY entry. Stages with no known marker are always reported not-done.
    Falls back to all-False if h5py is unavailable or the file can't be read.
    """
    done = {s: False for s in STAGE_ORDER}
    try:
        import h5py
    except Exception:
        return done, "h5py-unavailable"

    if not nxs_path or not os.path.isfile(nxs_path):
        return done, "no-nxs-file"

    ent_list = [e for e in (entries or "f1 f2 f3").split() if e.strip()]
    try:
        with h5py.File(nxs_path, "r") as f:
            present_entries = [e for e in ent_list if e in f]
            if not present_entries:
                present_entries = [e for e in f.keys()
                                   if e not in ("entry",) and isinstance(f.get(e), h5py.Group)]
            if not present_entries:
                return done, "no-entries"

            # root 'entry' group (if present) is where file-level process
            # records like nxcombine live.
            root_entry = f.get("entry") if "entry" in f else None

            for stage in STAGE_ORDER:
                markers = STAGE_MARKERS.get(stage, [])
                if not markers:
                    done[stage] = False          # no reliable signal → re-run
                    continue
                group_ok = stage in STAGE_MARKERS_GROUP_OK

                if stage in FILE_LEVEL_STAGES:
                    # whole-file step: look at the file root and the root
                    # 'entry' group rather than per-entry fN groups.
                    search_roots = [f]
                    if isinstance(root_entry, h5py.Group):
                        search_roots.append(root_entry)
                    done[stage] = any(
                        _h5_marker_present(r, m, group_ok)
                        for r in search_roots for m in markers)
                    continue

                all_entries_done = True
                for e in present_entries:
                    grp = f.get(e)
                    if grp is None:
                        all_entries_done = False
                        break
                    hit = any(_h5_marker_present(grp, m, group_ok) for m in markers)
                    if not hit:
                        all_entries_done = False
                        break
                done[stage] = all_entries_done
    except Exception as ex:
        return {s: False for s in STAGE_ORDER}, f"read-error: {ex}"

    return done, None


def _apply_monotonic(done, stages):
    """Fill in implied-complete stages using pipeline dependencies, scoped to
    `stages` only.

    Rule: a stage is implied done if any stage that DEPENDS on it is done.
    The dependency chain mostly follows pipeline order, with one exception:
    `copy` is NOT implied by `find` (find can complete without copy, e.g. in
    no-parent mode). `copy` is only implied when a copy-dependent stage
    (refine, transform, combine) is done.

    Examples (parent set load,link,copy,max,find,refine,transform,combine):
      - transform done  → everything before (incl. copy) filled  → remaining []
      - find done only  → load,link,max,find filled; copy NOT filled
                          → remaining [copy, refine, transform, combine]
    """
    out = {s: bool(done.get(s, False)) for s in stages}

    # Stages that, if complete, prove `copy` ran.
    COPY_DEPENDENTS = {"refine", "transform", "combine"}
    copy_implied = any(out.get(s, False) for s in COPY_DEPENDENTS)

    # Find the latest done stage, but ignore `copy` when deciding the fill point
    # so that a done `find` doesn't drag `copy` in via position.
    last_done_idx = -1
    for i, s in enumerate(stages):
        if out[s] and s != "copy":
            last_done_idx = i

    for i in range(last_done_idx + 1):
        s = stages[i]
        if s == "copy":
            # only fill copy if a copy-dependent stage is actually done
            out[s] = out[s] or copy_implied
        else:
            out[s] = True
    return out


def _remaining_steps(done, stages=None):
    """Return the stages NOT yet done, in pipeline order, after applying the
    monotonic rule within `stages` (defaults to the full STAGE_ORDER)."""
    stages = stages if stages is not None else STAGE_ORDER
    filled = _apply_monotonic(done, stages)
    return [s for s in stages if not filled.get(s, False)]


def _temp_nxs_path(user_dir, temp):
    """Find the .nxs file associated with a temperature.
    Convention: <USER_DIR>/<SAMPLE>_<temp>.nxs  (e.g. FeGe_250.nxs).
    Falls back to any *_<temp>.nxs in USER_DIR."""
    ud = user_dir.rstrip("/")
    sample = os.path.basename(ud)
    # primary guess: SAMPLE prefix is the parent sample name (strip trailing -ST-xx etc.)
    candidates = []
    try:
        for fn in os.listdir(ud):
            if fn.endswith(f"_{temp}.nxs") and os.path.isfile(os.path.join(ud, fn)):
                candidates.append(os.path.join(ud, fn))
    except Exception:
        pass
    return candidates[0] if candidates else None


# ─────────────────────────────────────────────────────────────────────────────
# Raw-data scan check — verifies that each temperature has its raw image folders
# before generating a job. Raw path is derived from USER_DIR by mapping:
#   id4baux → id4b   and   nxrefine → raw6M   then appending the temperature.
# ─────────────────────────────────────────────────────────────────────────────
RAW_REQUIRED_SCANS = 3        # number of scan subfolders required
RAW_MIN_IMAGES_PER_SCAN = 3650  # minimum .cbf count required per scan folder (>=)
RAW_IMAGE_EXT = ".cbf"


def _raw_temp_dir(user_dir, temp):
    """Map a nxrefine USER_DIR + temperature to its raw image directory."""
    ud = user_dir.rstrip("/")
    raw = ud.replace("/id4baux/", "/id4b/").replace("/nxrefine/", "/raw6M/")
    return os.path.join(raw, str(temp))


def _count_files_fast(folder, cap=None):
    """Count regular files in `folder` using os.scandir. Optional `cap` stops
    early once reached. Returns count or -1 on permission error."""
    n = 0
    try:
        with os.scandir(folder) as it:
            for de in it:
                if de.is_file():
                    n += 1
                    if cap is not None and n >= cap:
                        return n
    except PermissionError:
        return -1
    except FileNotFoundError:
        return 0
    return n


def _count_cbf_fast(folder, cap):
    """Count .cbf files in `folder` using os.scandir (no per-file stat).
    Stops counting once `cap` is reached (we only need to know it's >= the
    threshold). Returns the count, capped at `cap`, or -1 on permission error."""
    n = 0
    try:
        with os.scandir(folder) as it:
            for de in it:
                name = de.name
                if len(name) >= 4 and name[-4:].lower() == RAW_IMAGE_EXT:
                    # scandir gives is_file() without an extra stat in most cases
                    if de.is_file():
                        n += 1
                        if n >= cap:
                            return n
    except PermissionError:
        return -1
    except FileNotFoundError:
        return 0
    return n


def _check_raw_scans(user_dir, temp):
    """Check the raw folder for a temperature.

    Returns (ok, info) where info is a dict:
      {raw_dir, scans:[{name,count,ok}], n_scans, reason}
    ok is True only if there are >= RAW_REQUIRED_SCANS scan subfolders AND each
    contains at least RAW_MIN_IMAGES_PER_SCAN .cbf files.

    Fast path: uses os.scandir and stops counting each folder once the minimum
    threshold is reached (so a 3651-image folder costs ~3650 cheap iterations,
    not 3651 stat() syscalls)."""
    raw_dir = _raw_temp_dir(user_dir, temp)
    info = {"raw_dir": raw_dir, "scans": [], "n_scans": 0, "reason": None}

    # list subdirectories via scandir (single pass, type info included)
    try:
        subs = []
        with os.scandir(raw_dir) as it:
            for de in it:
                if de.is_dir():
                    subs.append(de.name)
        subs.sort()
    except FileNotFoundError:
        info["reason"] = "raw folder not found"
        return False, info
    except NotADirectoryError:
        info["reason"] = "raw folder not found"
        return False, info
    except PermissionError:
        info["reason"] = "permission denied"
        return False, info

    info["n_scans"] = len(subs)

    # Early exit: too few scan folders — skip the expensive counting entirely.
    if len(subs) < RAW_REQUIRED_SCANS:
        info["reason"] = f"only {len(subs)} scan folder(s), need {RAW_REQUIRED_SCANS}"
        # still record names (counts unknown/!) cheaply
        info["scans"] = [{"name": d, "count": None, "ok": False} for d in subs]
        return False, info

    all_ok = True
    cap = RAW_MIN_IMAGES_PER_SCAN
    for d in subs:
        cbf = _count_cbf_fast(os.path.join(raw_dir, d), cap)
        scan_ok = (cbf >= RAW_MIN_IMAGES_PER_SCAN)
        if not scan_ok:
            all_ok = False
        info["scans"].append({"name": d, "count": cbf, "ok": scan_ok})

    if not all_ok:
        bad = [f"{s['name']} ({s['count']})" for s in info["scans"] if not s["ok"]]
        info["reason"] = (f"scan(s) under {RAW_MIN_IMAGES_PER_SCAN} {RAW_IMAGE_EXT}: "
                          + ", ".join(bad))
        return False, info

    return True, info


@app.route("/watch")
def watch():
    """Return JSON with per-temp file counts, .nxs stage status, and qstat."""
    user_dir = request.args.get("user_dir", "").strip().rstrip("/")
    entries  = request.args.get("entries", "f1 f2 f3").strip() or "f1 f2 f3"
    result = {"temps": [], "qstat": None, "qstat_error": None, "user_dir": user_dir,
              "stage_order": STAGE_ORDER}

    # Parent availability decides which stage set is relevant for display
    has_parent = False
    if user_dir and os.path.isdir(user_dir):
        try:
            has_parent = any(f.endswith("_parent.nxs")
                             for f in os.listdir(user_dir))
        except Exception:
            has_parent = False
    relevant_stages = STAGE_ORDER if has_parent else NO_PARENT_STAGES
    result["has_parent"] = has_parent
    result["relevant_stages"] = relevant_stages

    # ── file counts + .nxs stage status per numeric temp subdir ───────────────
    if user_dir and os.path.isdir(user_dir):
        try:
            names = []
            with os.scandir(user_dir) as it:
                for de in it:
                    if de.is_dir() and de.name.lstrip('-').isdigit():
                        names.append(de.name)
            names.sort(key=lambda x: int(x))
            for name in names:
                full = os.path.join(user_dir, name)
                nfiles = _count_files_fast(full)   # watcher shows the real count
                # read .nxs stages
                nxs = _temp_nxs_path(user_dir, name)
                done, note = _detect_completed_stages(nxs, entries)
                filled = _apply_monotonic(done, relevant_stages)
                result["temps"].append({
                    "temp": name,
                    "files": nfiles,
                    "nxs": os.path.basename(nxs) if nxs else None,
                    "stages": {s: bool(filled.get(s, False)) for s in relevant_stages},
                    "note": note,
                })
        except Exception as e:
            result["dir_error"] = str(e)

    # ── qstat ─────────────────────────────────────────────────────────────────
    try:
        res = subprocess.run(["qstat"], capture_output=True, text=True, timeout=10)
        out = res.stdout.strip() or "(no jobs in queue)"
        result["qstat"] = out
        result["qstat_lines"] = out.split("\n")
        if res.stderr.strip():
            result["qstat_error"] = res.stderr.strip()
    except FileNotFoundError:
        result["qstat_error"] = "qstat not found — SGE environment not loaded."
        result["qstat_lines"] = []
    except Exception as e:
        result["qstat_error"] = str(e)
        result["qstat_lines"] = []

    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Auto-Watch — polls raw6M; when a temperature has 3 valid raw scan folders it
# runs chmod on the PI folder, auto-generates the appropriate script, and
# (optionally) submits it. Each temp is handled once per stage (noparent/parent).
# ─────────────────────────────────────────────────────────────────────────────
def _autowatch_log_add(temp, stage, action, ok, detail):
    with _autowatch_lock:
        _autowatch_log.append({
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "temp": temp, "stage": stage, "action": action,
            "ok": ok, "detail": detail,
        })
        # keep last 200
        if len(_autowatch_log) > 200:
            del _autowatch_log[:len(_autowatch_log) - 200]


@app.route("/autowatch_poll", methods=["POST"])
def autowatch_poll():
    """One polling cycle. For each numeric temp under USER_DIR whose raw6M folder
    has the required valid scans, run chmod + generate (+ optional submit) if not
    already handled for the current stage. Returns JSON with actions taken and log."""
    user_dir   = (request.form.get("user_dir", "") or "").strip().rstrip("/")
    chmod_dir  = (request.form.get("chmod_dir", "") or "").strip()
    mode       = (request.form.get("mode", "generate") or "generate").strip()  # generate | submit
    entries    = (request.form.get("entries", "f1 f2 f3") or "f1 f2 f3").strip()
    threshold  = (request.form.get("find_threshold", "80000") or "80000").strip()
    script_dir = (request.form.get("script_dir", "") or "").strip()
    q     = (request.form.get("q_queue", "all.q") or "all.q").strip()
    mem   = (request.form.get("q_mem", "300G") or "300G").strip()
    pe    = (request.form.get("q_pe", "sge_pe") or "sge_pe").strip()
    cores = (request.form.get("q_cores", "12") or "12").strip()

    out = {"actions": [], "error": None}
    if not user_dir or not os.path.isdir(user_dir):
        out["error"] = "USER_DIR not found."
        return jsonify(out)

    # parent presence decides stage + step chain
    try:
        has_parent = any(f.endswith("_parent.nxs") for f in os.listdir(user_dir))
    except Exception:
        has_parent = False
    stage = "parent" if has_parent else "noparent"

    full_steps      = ["load", "link", "copy", "max", "find", "refine", "transform", "combine"]
    no_parent_steps = list(NO_PARENT_STAGES)   # load, link, max, find
    steps = full_steps if has_parent else no_parent_steps

    sdir = script_dir or _default_script_dir(user_dir)
    ud = user_dir.rstrip("/") + "/"

    # iterate numeric temps
    try:
        names = sorted([n for n in os.listdir(user_dir)
                        if n.lstrip('-').isdigit()
                        and os.path.isdir(os.path.join(user_dir, n))], key=int)
    except Exception as e:
        out["error"] = f"Cannot list USER_DIR: {e}"
        return jsonify(out)

    csv_results = []
    # Snapshot the full job names already in the SGE queue (any user) so we can
    # recognise a temperature whose script was already submitted elsewhere —
    # e.g. by hand from lnx16 — and avoid submitting a duplicate.
    active_names = _active_job_names()
    for temp in names:
        key = f"{user_dir}|{temp}:{stage}"
        with _autowatch_lock:
            if key in _autowatch_handled:
                continue
        # Don't resubmit a temperature whose job is already queued/running
        # (submitted here, from lnx16, or under another account — qstat -u '*'
        # sees them all).
        base_chk = _script_name(ud, temp)
        if active_names is not None and _job_already_active(base_chk, active_names):
            _autowatch_log_add(temp, stage, "skip-duplicate", True,
                               f"{base_chk} already in the SGE queue — not resubmitting")
            continue
        # check raw scans
        raw_ok, raw_info = _check_raw_scans(user_dir, temp)
        if not raw_ok:
            continue   # not ready yet — will re-check next poll

        # Stage-aware: run ONLY the steps this temperature still needs, so
        # finished stages (e.g. load/link/max/find) are never redone. Falls back
        # to the full chain if detection fails (no .nxs / no h5py).
        relevant = full_steps if has_parent else no_parent_steps
        try:
            _nxs = _temp_nxs_path(user_dir, temp)
            _done, _ = _detect_completed_stages(_nxs, entries)
            if _done:
                # Detection produced real info: run ONLY the remaining stages.
                # If nothing remains, _remaining_steps returns [] -> skip below.
                temp_steps = _remaining_steps(_done, relevant)
            else:
                # Nothing detected (fresh data / no markers) -> run the full chain.
                temp_steps = list(relevant)
        except Exception:
            # Detection failed (no .nxs yet / no h5py) -> run the full chain.
            temp_steps = list(relevant)
        if not temp_steps:
            _autowatch_log_add(temp, stage, "skip-complete", True,
                               f"{base_chk}: all stages already done")
            continue

        # Reserve this (temp, stage) ATOMICALLY so two overlapping poll cycles
        # can't both submit it (that caused the duplicate jobs).
        with _autowatch_lock:
            if key in _autowatch_handled:
                continue
            _autowatch_handled.add(key)

        action = {"temp": temp, "stage": stage, "steps": list(temp_steps),
                  "chmod_ok": False, "script_path": None, "submitted": False,
                  "error": None}

        # 1) chmod -R 777 on PI folder
        if chmod_dir:
            try:
                cres = subprocess.run(["chmod", "-R", "777", chmod_dir],
                                      capture_output=True, text=True, timeout=120)
                action["chmod_ok"] = (cres.returncode == 0)
                if cres.returncode != 0:
                    action["error"] = f"chmod rc={cres.returncode}: {cres.stderr.strip()[:120]}"
            except Exception as ex:
                action["error"] = f"chmod failed: {ex}"

        # 2) generate script
        txt = _build_script(ud, temp, entries, temp_steps, threshold, chmod_dir or "")
        base = _script_name(ud, temp)
        spath, warn = _save_one_script(txt, sdir, base)
        action["script_path"] = spath
        if warn:
            action["error"] = (action["error"] + " | " if action["error"] else "") + warn

        # 3) optional submit
        if spath and mode == "submit":
            qcmd = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {spath}"
            try:
                sres = _run_qsub(qcmd, _ssh_cfg_from_form(request.form), timeout=30)
                action["submitted"] = (sres.returncode == 0)
                action["job"] = sres.stdout.strip() if sres.returncode == 0 else sres.stderr.strip()[:120]
                action["job_id"] = _extract_job_id(sres.stdout) if sres.returncode == 0 else None
                _record_submitted(action["job_id"])
            except Exception as ex:
                action["error"] = (action["error"] + " | " if action["error"] else "") + f"qsub failed: {ex}"

        # mark handled (once per stage) and log
        with _autowatch_lock:
            _autowatch_handled.add(key)
        _autowatch_log_add(temp, stage,
                           ("chmod+generate+submit" if mode == "submit" else "chmod+generate"),
                           action["error"] is None,
                           action.get("job") or os.path.basename(spath) if spath else (action["error"] or ""))
        if spath:
            qcmd_full = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {spath}"
            csv_results.append({"temp": temp, "script_path": spath, "qsub_cmd": qcmd_full})
        out["actions"].append(action)

    # write/refresh CSV if anything was produced this cycle
    if csv_results:
        try:
            _write_autoscan_csv(csv_results, sdir, _csv_basename(user_dir))
        except Exception:
            pass

    with _autowatch_lock:
        out["log"] = list(_autowatch_log[-40:])
        out["handled_count"] = len(_autowatch_handled)
    out["stage"] = stage
    out["has_parent"] = has_parent
    return jsonify(out)


@app.route("/autowatch_reset", methods=["POST"])
def autowatch_reset():
    """Clear the handled set and log so temps can be auto-processed again."""
    with _autowatch_lock:
        _autowatch_handled.clear()
        _autowatch_log.clear()
    return jsonify({"ok": True})


def _detect_parent_temp(user_dir):
    """Best-effort auto-detection of the parent temperature to skip.

    The `*_parent.nxs` file in USER_DIR is normally a symlink that points into
    the parent temperature's own folder (e.g. .../300/<sample>.nxs), so we
    resolve it and return the numeric temperature folder from the resolved path
    — but only if that number is an actual directory under USER_DIR, so we never
    guess wrong from an unrelated number in the path. Returns None when there is
    no parent file yet or the target can't be mapped to a real temp folder, in
    which case the caller falls back to the manually typed value."""
    try:
        parents = [f for f in os.listdir(user_dir) if f.endswith("_parent.nxs")]
    except Exception:
        return None
    for pf in parents:
        target = os.path.realpath(os.path.join(user_dir, pf))
        for part in target.replace("\\", "/").split("/"):
            if part.lstrip("-").isdigit() and os.path.isdir(os.path.join(user_dir, part)):
                return part
    return None


def _autosubmit_log_add(temp, stage, action, ok, detail):
    with _autosubmit_lock:
        _autosubmit_log.append({
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "temp": temp, "stage": stage, "action": action,
            "ok": ok, "detail": detail,
        })
        if len(_autosubmit_log) > 200:
            del _autosubmit_log[:len(_autosubmit_log) - 200]


@app.route("/autosubmit_poll", methods=["POST"])
def autosubmit_poll():
    """One Auto-Submit cycle — a SEPARATE auto-submitter from Auto-Watch.

    Same parent-aware stage logic (no parent file yet -> load/link/max/find;
    parent file present -> full chain load/link/copy/max/find/refine/transform/
    combine), but it SKIPS the parent temperature (default 300) because that
    folder is the parent, not a child dataset. For every other numeric temp whose
    raw scans are ready it runs chmod + generate (+ optional submit), once per
    stage. Uses its own handled set/log so it never collides with Auto-Watch."""
    user_dir   = (request.form.get("user_dir", "") or "").strip().rstrip("/")
    chmod_dir  = (request.form.get("chmod_dir", "") or "").strip()
    mode       = (request.form.get("mode", "generate") or "generate").strip()  # generate | submit
    entries    = (request.form.get("entries", "f1 f2 f3") or "f1 f2 f3").strip()
    threshold  = (request.form.get("find_threshold", "80000") or "80000").strip()
    script_dir = (request.form.get("script_dir", "") or "").strip()
    parent_raw  = (request.form.get("parent_temp", "300") or "300").strip()
    auto_parent = (request.form.get("auto_parent", "1") or "1").strip() == "1"
    q     = (request.form.get("q_queue", "all.q") or "all.q").strip()
    mem   = (request.form.get("q_mem", "300G") or "300G").strip()
    pe    = (request.form.get("q_pe", "sge_pe") or "sge_pe").strip()
    cores = (request.form.get("q_cores", "12") or "12").strip()

    out = {"actions": [], "error": None, "parent_temp": parent_raw}
    if not user_dir or not os.path.isdir(user_dir):
        out["error"] = "USER_DIR not found."
        return jsonify(out)

    # parent presence decides stage + step chain
    try:
        has_parent = any(f.endswith("_parent.nxs") for f in os.listdir(user_dir))
    except Exception:
        has_parent = False
    stage = "parent" if has_parent else "noparent"

    # Which temperature folder is the parent to skip. Prefer AUTO-DETECTION from
    # the *_parent.nxs file (it normally links into the parent temperature's
    # folder); fall back to the typed value when nothing can be detected yet
    # (e.g. before the parent file exists). Compared numerically so "300",
    # "300.0" and a "300" folder all match.
    detected_parent = _detect_parent_temp(user_dir) if auto_parent else None
    effective_parent = detected_parent if detected_parent else parent_raw
    try:
        parent_val = int(float(effective_parent))
    except Exception:
        parent_val = None
    out["parent_temp"]     = effective_parent
    out["parent_detected"] = detected_parent
    out["parent_source"]   = "auto" if detected_parent else "manual"

    full_steps      = ["load", "link", "copy", "max", "find", "refine", "transform", "combine"]
    no_parent_steps = list(NO_PARENT_STAGES)   # load, link, max, find
    steps = full_steps if has_parent else no_parent_steps

    sdir = script_dir or _default_script_dir(user_dir)
    ud = user_dir.rstrip("/") + "/"

    # iterate numeric temps
    try:
        names = sorted([n for n in os.listdir(user_dir)
                        if n.lstrip('-').isdigit()
                        and os.path.isdir(os.path.join(user_dir, n))], key=int)
    except Exception as e:
        out["error"] = f"Cannot list USER_DIR: {e}"
        return jsonify(out)

    csv_results = []
    skipped_parent = []
    requeued = []
    # Snapshot the cluster's active job ids once per cycle so we can spot temps
    # whose submitted job was cancelled (`qdel`) or died without producing output.
    active_ids = _active_job_ids()
    # Full job names already in the SGE queue (any user), to detect a temp whose
    # script was already submitted elsewhere (e.g. by hand from lnx16).
    active_names = _active_job_names()
    for temp in names:
        # skip the parent temperature (default 300): it's the parent, not a child
        is_parent_temp = False
        if parent_val is not None:
            try:
                is_parent_temp = (int(temp) == parent_val)
            except Exception:
                is_parent_temp = (temp == parent_raw)
        else:
            is_parent_temp = (temp == parent_raw)
        if is_parent_temp:
            skipped_parent.append(temp)
            continue

        key = f"{user_dir}|{temp}:{stage}"
        with _autosubmit_lock:
            already = key in _autosubmit_handled
            tracked_job = _autosubmit_jobs.get(key)
        if already:
            # Normally skip an already-handled temp — UNLESS its submitted job was
            # cancelled (`qdel -f <id>`) or died WITHOUT producing output, in which
            # case clear it so this cycle resubmits that temperature.
            if _should_resubmit(user_dir, temp, tracked_job, active_ids):
                with _autosubmit_lock:
                    _autosubmit_handled.discard(key)
                    _autosubmit_jobs.pop(key, None)
                requeued.append(temp)
                _autosubmit_log_add(temp, stage, "requeue", True,
                                    f"job {tracked_job or '?'} gone with no output — resubmitting")
                # fall through and reprocess this temp
            else:
                continue
        # Don't resubmit a temperature whose job is already queued/running —
        # whether it was submitted here, by hand from lnx16, or under another
        # account (qstat -u '*' sees them all). This stops the same temp going
        # in twice when a local and a remote submitter are both in play.
        base_chk = _script_name(ud, temp)
        if active_names is not None and _job_already_active(base_chk, active_names):
            _autosubmit_log_add(temp, stage, "skip-duplicate", True,
                                f"{base_chk} already in the SGE queue — not resubmitting")
            continue
        # check raw scans
        raw_ok, raw_info = _check_raw_scans(user_dir, temp)
        if not raw_ok:
            continue   # not ready yet — will re-check next poll

        # Stage-aware: run ONLY the steps this temperature still needs. Read its
        # .nxs to see which stages already completed and submit just the rest, so
        # finished work (e.g. load/link/max/find) is never redone. Falls back to
        # the full chain if detection fails (no .nxs / no h5py).
        relevant = full_steps if has_parent else no_parent_steps
        try:
            _nxs = _temp_nxs_path(user_dir, temp)
            _done, _ = _detect_completed_stages(_nxs, entries)
            if _done:
                # Detection produced real info: run ONLY the remaining stages.
                # If nothing remains, _remaining_steps returns [] -> skip below.
                temp_steps = _remaining_steps(_done, relevant)
            else:
                # Nothing detected (fresh data / no markers) -> run the full chain.
                temp_steps = list(relevant)
        except Exception:
            # Detection failed (no .nxs yet / no h5py) -> run the full chain.
            temp_steps = list(relevant)
        if not temp_steps:
            _autosubmit_log_add(temp, stage, "skip-complete", True,
                                f"{base_chk}: all stages already done")
            continue

        # Reserve this (temp, stage) ATOMICALLY before doing any work, so if two
        # poll cycles overlap (the browser fired the next poll before this one
        # finished) only ONE submits it — this is what was producing duplicate
        # jobs for the same temperature.
        with _autosubmit_lock:
            if key in _autosubmit_handled:
                continue
            _autosubmit_handled.add(key)

        action = {"temp": temp, "stage": stage, "steps": list(temp_steps),
                  "chmod_ok": False, "script_path": None, "submitted": False,
                  "error": None}

        # 1) chmod -R 777 on PI folder
        if chmod_dir:
            try:
                cres = subprocess.run(["chmod", "-R", "777", chmod_dir],
                                      capture_output=True, text=True, timeout=120)
                action["chmod_ok"] = (cres.returncode == 0)
                if cres.returncode != 0:
                    action["error"] = f"chmod rc={cres.returncode}: {cres.stderr.strip()[:120]}"
            except Exception as ex:
                action["error"] = f"chmod failed: {ex}"

        # 2) generate script
        txt = _build_script(ud, temp, entries, temp_steps, threshold, chmod_dir or "")
        base = _script_name(ud, temp)
        spath, warn = _save_one_script(txt, sdir, base)
        action["script_path"] = spath
        if warn:
            action["error"] = (action["error"] + " | " if action["error"] else "") + warn

        # 3) optional submit
        if spath and mode == "submit":
            qcmd = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {spath}"
            try:
                sres = _run_qsub(qcmd, _ssh_cfg_from_form(request.form), timeout=30)
                action["submitted"] = (sres.returncode == 0)
                action["job"] = sres.stdout.strip() if sres.returncode == 0 else sres.stderr.strip()[:120]
                action["job_id"] = _extract_job_id(sres.stdout) if sres.returncode == 0 else None
                _record_submitted(action["job_id"])
            except Exception as ex:
                action["error"] = (action["error"] + " | " if action["error"] else "") + f"qsub failed: {ex}"

        # mark handled (once per stage), remember the job id for requeue
        # detection (so a later qdel can be noticed), and log
        with _autosubmit_lock:
            _autosubmit_handled.add(key)
            _jid = action.get("job_id")
            if _jid:
                _autosubmit_jobs[key] = _jid
            else:
                _autosubmit_jobs.pop(key, None)
        _autosubmit_log_add(temp, stage,
                            ("chmod+generate+submit" if mode == "submit" else "chmod+generate"),
                            action["error"] is None,
                            action.get("job") or os.path.basename(spath) if spath else (action["error"] or ""))
        if spath:
            qcmd_full = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {spath}"
            csv_results.append({"temp": temp, "script_path": spath, "qsub_cmd": qcmd_full})
        out["actions"].append(action)

    # write/refresh CSV if anything was produced this cycle
    if csv_results:
        try:
            _write_autoscan_csv(csv_results, sdir, _csv_basename(user_dir))
        except Exception:
            pass

    with _autosubmit_lock:
        out["log"] = list(_autosubmit_log[-40:])
        out["handled_count"] = len(_autosubmit_handled)
    out["stage"] = stage
    out["has_parent"] = has_parent
    out["skipped_parent"] = skipped_parent
    out["requeued"] = requeued
    return jsonify(out)


@app.route("/autosubmit_reset", methods=["POST"])
def autosubmit_reset():
    """Clear the Auto-Submit handled set and log (independent of Auto-Watch)."""
    with _autosubmit_lock:
        _autosubmit_handled.clear()
        _autosubmit_log.clear()
        _autosubmit_jobs.clear()
    return jsonify({"ok": True})


@app.route("/job_health", methods=["POST"])
def job_health():
    """Report jobs that did NOT complete properly, for the bottom-of-page table.

    ONLY jobs submitted from this app (any panel, any account) are considered —
    other people's cluster jobs are never shown. Two sources are merged:
      1) Error / held / suspended state, read from qstat but filtered to the job
         ids this app submitted (_submitted_job_ids).
      2) Auto-Submit tracked jobs that left the queue WITHOUT producing .nxs
         output (cancelled with qdel, or died) — i.e. 'gone, no output'.

    An optional USER_DIR scopes the 'gone, no output' rows to one sample folder."""
    user_dir = (request.form.get("user_dir", "") or "").strip().rstrip("/")

    out = {"jobs": [], "error": None, "qstat_ok": True,
           "checked_at": time.strftime("%H:%M:%S")}

    with _submitted_lock:
        mine = set(_submitted_job_ids)

    probs = _problem_jobs()
    if probs is None:
        out["qstat_ok"] = False
        out["error"] = "qstat is not available on this host, so SGE job states can't be read."
        probs = []
    # Keep only OUR jobs — never surface other users' error/held/suspended jobs.
    probs = [j for j in probs if str(j.get("id")) in mine]
    out["jobs"].extend(probs)

    # 'gone, no output': an Auto-Submit job we tracked is no longer in the queue
    # and its temperature folder still has no .nxs output.
    active = _active_job_ids()
    if active is not None:
        with _autosubmit_lock:
            tracked = list(_autosubmit_jobs.items())
        for key, jid in tracked:
            if not jid or str(jid) in active:
                continue
            try:
                ud_part, rest = key.split("|", 1)
                temp = rest.split(":", 1)[0]
            except Exception:
                ud_part, temp = "", ""
            if user_dir and ud_part.rstrip("/") != user_dir:
                continue
            if _temp_has_output(ud_part, temp):
                continue
            out["jobs"].append({
                "id": str(jid), "name": "", "full_name": "",
                "temp": temp, "user": "", "state": "gone", "queue": "",
                "submit": "", "folder": ud_part,
                "category": "gone-no-output",
                "issue": "Left the queue with no .nxs output — looks cancelled/failed; needs resubmit.",
            })

    out["count"] = len(out["jobs"])
    return jsonify(out)


@app.route("/scan_temps")
def scan_temps():
    """Scan USER_DIR for numeric temp subdirs needing processing.

    Default mode: file-count heuristic (<10 files → process).
    Exact mode (?exact=1): open each temperature's .nxs and detect which
    pipeline stages are already complete, returning the remaining steps so the
    generated script can resume where it left off."""
    user_dir = request.args.get("user_dir", "").strip().rstrip("/")
    exact    = request.args.get("exact", "0") == "1"
    entries  = request.args.get("entries", "f1 f2 f3").strip() or "f1 f2 f3"
    if not user_dir:
        return jsonify({"error": "No USER_DIR provided."})
    if not os.path.isdir(user_dir):
        return jsonify({"error": f"Directory not found: {user_dir}"})

    # Check for parent file — no longer an error if missing; switch to no-parent mode
    parent_files = [f for f in os.listdir(user_dir)
                    if f.endswith("_parent.nxs") and os.path.isfile(os.path.join(user_dir, f))]
    has_parent = bool(parent_files)
    parent_file = parent_files[0] if parent_files else None

    # Stages relevant for completeness depend on parent availability:
    #   parent → full pipeline; no parent → only load, link, max, find
    relevant_stages = STAGE_ORDER if has_parent else NO_PARENT_STAGES

    try:
        entry_names = sorted(os.listdir(user_dir))
    except PermissionError:
        return jsonify({"error": f"Permission denied reading {user_dir}"})

    if exact:
        # ── EXACT mode: inspect each temp's .nxs for completed stages ──────────
        details = []          # per-temp: {temp, nxs, done{}, remaining[], note}
        to_process = []
        already_done = []
        raw_skipped = []
        h5_note = None
        for name in entry_names:
            if not name.lstrip('-').isdigit():
                continue
            full = os.path.join(user_dir, name)
            if not os.path.isdir(full):
                continue
            nxs = _temp_nxs_path(user_dir, name)
            done, note = _detect_completed_stages(nxs, entries)
            if note in ("h5py-unavailable",):
                h5_note = note
            # apply monotonic rule scoped to the relevant stage set
            filled = _apply_monotonic(done, relevant_stages)
            remaining = [s for s in relevant_stages if not filled.get(s, False)]
            entry = {
                "temp": name,
                "nxs": os.path.basename(nxs) if nxs else None,
                "done": filled,
                "remaining": remaining,
                "note": note,
            }
            details.append(entry)
            if not remaining:
                already_done.append(name)
            else:
                # needs processing — but only if raw scans are present
                raw_ok, raw_info = _check_raw_scans(user_dir, name)
                if raw_ok:
                    to_process.append(name)
                else:
                    raw_skipped.append({"temp": name, "reason": raw_info["reason"],
                                        "raw_dir": raw_info["raw_dir"],
                                        "n_scans": raw_info["n_scans"]})
        # sort numerically
        details.sort(key=lambda d: int(d["temp"]))
        to_process.sort(key=int)
        already_done.sort(key=int)
        raw_skipped.sort(key=lambda d: int(d["temp"]))
        return jsonify({
            "exact": True,
            "has_parent": has_parent,
            "parent_file": parent_file,
            "stage_order": relevant_stages,
            "details": details,
            "to_process": to_process,
            "already_done": already_done,
            "raw_skipped": raw_skipped,
            "h5_note": h5_note,
        })

    # ── DEFAULT mode: file-count heuristic ────────────────────────────────────
    to_process, already_done, raw_skipped = [], [], []
    for name in entry_names:
        if not name.lstrip('-').isdigit():
            continue
        full = os.path.join(user_dir, name)
        n = _count_files_fast(full, cap=10)   # only need <10 vs >=10
        if n < 0:
            continue          # no access — leave out
        elif n >= 10:
            already_done.append(name)   # complete — skip
        else:
            # needs processing — but only if raw scans are present
            raw_ok, raw_info = _check_raw_scans(user_dir, name)
            if raw_ok:
                to_process.append(name)
            else:
                raw_skipped.append({"temp": name, "reason": raw_info["reason"],
                                    "raw_dir": raw_info["raw_dir"],
                                    "n_scans": raw_info["n_scans"]})

    return jsonify({
        "exact": False,
        "has_parent": has_parent,
        "parent_file": parent_file,
        "to_process":  to_process,
        "already_done": already_done,
        "raw_skipped": raw_skipped,
        "empty":       [],
    })


@app.route("/browse_dir")
def browse_dir():
    """Return JSON list of immediate subdirectories under path, confined to /nfs/chess/."""
    BROWSE_ROOT = "/nfs/chess"
    path = request.args.get("path", ROOT)
    path = os.path.normpath(path)
    # Safety: never escape /nfs/chess
    if not path.startswith(BROWSE_ROOT):
        path = ROOT
    dirs = []
    try:
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            if os.path.isdir(full):
                dirs.append({"name": name, "path": full})
    except PermissionError:
        pass
    except FileNotFoundError:
        path = ROOT
    return jsonify({"path": path, "dirs": dirs})


@app.route("/", methods=["GET"])
def index():
    return _render(
        errors=[], script_text=None, script_path=None, qsub_cmd=None,
        job_output=None, job_stderr=None, job_ok=False,
        pre_chmod_output=None, pre_chmod_ok=False,
        pre_chmod_enable="0", pre_chmod_path="",
        # default form values
        user_dir="", entries="f1 f2 f3", temp_mode="single",
        temp_single="", temp_start="", temp_end="", temp_step="1",
        temp_list="100,105,110", find_threshold="80000",
        q_queue="all.q", q_mem="300G", q_pe="sge_pe", q_cores="12",
        script_dir="", chmod_path="",
        notify_email="", notify_end="1", notify_abort="0", notify_begin="0",
        step_load="1", step_link_copy="1", step_link_only="0",
        step_link_ow="0", step_copy_only="0", step_find="1", step_refine="1",
        step_transform="1", step_pdf="0",
    )


@app.route("/", methods=["POST"])
def submit():
    fv = {k: request.form.get(k, "") for k in [
        "user_dir","entries","temp_mode","temp_single","temp_start","temp_end",
        "temp_step","temp_list","find_threshold","q_queue","q_mem","q_pe",
        "q_cores","script_dir","chmod_path","notify_email","autoscan_selected","autoscan_exact",
        "pre_chmod_path","remote_host","remote_user","remote_key",
    ]}
    for s in ["load","link_copy","link_only","link_ow","copy_only","find","refine","transform","pdf"]:
        fv[f"step_{s}"] = request.form.get(f"step_{s}", "0")
    for n in ["notify_end","notify_abort","notify_begin"]:
        fv[n] = request.form.get(n, "0")
    fv["pre_chmod_enable"] = request.form.get("pre_chmod_enable", "0")
    fv["remote_enable"] = request.form.get("remote_enable", "0")
    action = request.form.get("action", "preview")

    errors, script_text, script_path, qsub_cmd = [], None, None, None
    job_output, job_stderr, job_ok = None, None, False
    pre_chmod_output, pre_chmod_ok = None, False

    # Standalone chmod: the "Run chmod now" button submits action="chmod",
    # running the chmod on its own without generating or submitting a job.
    if action == "chmod":
        if not fv.get("pre_chmod_path", "").strip():
            pre_chmod_ok, pre_chmod_output = False, "Enter a path to chmod first."
        else:
            pre_chmod_ok, pre_chmod_output = _do_chmod_777(fv.get("pre_chmod_path", ""))
        return _render(errors=[], script_text=None, script_path=None,
                       qsub_cmd=None, job_output=None, job_stderr=None, job_ok=False,
                       pre_chmod_output=pre_chmod_output, pre_chmod_ok=pre_chmod_ok,
                       **fv)

    # ── validate ──────────────────────────────────────────────────────────────
    user_dir  = fv["user_dir"].strip()

    # Auto-normalize: if USER_DIR ends with a numeric directory (e.g. .../CeCd3P3_b3_s1/91/)
    # strip that component and use it as the temperature, so the script name is correct.
    _ud_last = os.path.basename(user_dir.rstrip("/"))
    if _ud_last.lstrip('-').isdigit() and _ud_last:
        user_dir = os.path.dirname(user_dir.rstrip("/")).rstrip("/") + "/"
        fv["user_dir"] = user_dir   # fix the re-rendered form value too
        # Auto-fill single temperature if none was entered
        if not fv.get("temp_single","").strip():
            fv["temp_single"] = _ud_last
            fv["temp_mode"]   = "single"

    if not user_dir.strip("/"):
        errors.append("Sample directory (USER_DIR) is required.")

    # ── AUTO-SCAN branch ──────────────────────────────────────────────────────
    if fv["temp_mode"] == "autoscan" and not errors:
        return _submit_autoscan(fv, user_dir, action)

    steps = [s for s in ["load","link_copy","link_only","link_ow","copy_only","find","refine","transform","pdf"]
             if fv.get(f"step_{s}") == "1"]
    if not steps:
        errors.append("Select at least one processing step.")

    temps_str, terr = _parse_temps(
        fv["temp_mode"], fv["temp_single"], fv["temp_start"],
        fv["temp_end"],  fv["temp_step"],  fv["temp_list"])
    if terr:
        errors.append(terr)

    if not errors:
        threshold  = fv["find_threshold"].strip() or "80000"
        chmod_path = fv["chmod_path"].strip()

        # ── build script ──────────────────────────────────────────────────────
        script_text = _build_script(
            user_dir, temps_str, fv["entries"], steps, threshold, chmod_path,
            notify_email=fv.get("notify_email",""),
            notify_end=fv.get("notify_end","0"),
            notify_abort=fv.get("notify_abort","0"),
            notify_begin=fv.get("notify_begin","0"),
        )

        # ── save script ───────────────────────────────────────────────────────
        sdir  = fv["script_dir"].strip()
        if not sdir:
            parent = os.path.dirname(user_dir.rstrip("/")) if user_dir.rstrip("/") else ""
            sdir   = os.path.join(parent, "scripts") if parent else "/tmp/nxreduce_scripts"
        base_name   = _script_name(user_dir, temps_str)
        # Avoid overwriting existing scripts: add _2, _3 … suffix
        sname       = base_name + ".sh"
        candidate   = os.path.join(sdir, sname)
        _counter    = 2
        try:
            os.makedirs(sdir, exist_ok=True)
            while os.path.exists(candidate):
                sname     = f"{base_name}_{_counter}.sh"
                candidate = os.path.join(sdir, sname)
                _counter += 1
        except Exception:
            pass  # makedirs error handled below
        script_path = candidate
        _save_warn  = None
        try:
            os.makedirs(sdir, exist_ok=True)
            with open(script_path, "w") as f:
                f.write(script_text)
            os.chmod(script_path, 0o755)
        except Exception as _io_err:
            # Fall back to /tmp so the user can still see and copy the script
            _fallback = f"/tmp/nxreduce_scripts"
            try:
                os.makedirs(_fallback, exist_ok=True)
                script_path = os.path.join(_fallback, sname)
                with open(script_path, "w") as f:
                    f.write(script_text)
                os.chmod(script_path, 0o755)
                _save_warn = (f"Could not write to {sdir!r} ({_io_err}). "
                              f"Script saved to fallback: {script_path}")
            except Exception as _fb_err:
                errors.append(f"Script save failed: {_io_err} | fallback also failed: {_fb_err}")
                script_text = None
        if _save_warn:
            errors.append(_save_warn)

        # ── build qsub command ────────────────────────────────────────────────
        if script_text is not None:
            q, mem, pe, cores = (fv["q_queue"].strip()  or "all.q",
                                 fv["q_mem"].strip()    or "300G",
                                 fv["q_pe"].strip()     or "sge_pe",
                                 fv["q_cores"].strip()  or "12")
            qsub_cmd = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {script_path}"

        # ── submit ────────────────────────────────────────────────────────────
        if (action == "submit" and script_text is not None
                and fv.get("pre_chmod_enable") == "1"):
            pre_chmod_ok, pre_chmod_output = _do_chmod_777(fv.get("pre_chmod_path", ""))

        if action == "submit" and script_text is not None:
            _ssh_cfg = _ssh_cfg_from_form(request.form)
            try:
                res = _run_qsub(qsub_cmd, _ssh_cfg, timeout=30)
                job_output = res.stdout.strip() or "(no stdout)"
                job_stderr = res.stderr.strip()
                job_ok     = res.returncode == 0
                if job_ok:
                    _record_submitted(_extract_job_id(res.stdout))
                if _ssh_cfg and job_ok:
                    job_output += f"\n(submitted as {_ssh_cfg['user']}@{_ssh_cfg['host']})"
            except FileNotFoundError:
                job_output = ("qsub not found — is the SGE environment loaded? "
                              "Run: source /nfs/chess/id4baux/nxserver/nxsetup.sh")
                job_ok = False
            except Exception as ex:
                job_output = str(ex)
                job_ok     = False

            # ── record in history ─────────────────────────────────────────────
            step_labels = {
                "load":"Load","link_copy":"Link+Copy","link_only":"Link+Max",
                "link_ow":"Link(ow)","copy_only":"Copy","find":"Find",
                "refine":"Refine","transform":"Transform","pdf":"PDF"}
            with _history_lock:
                _job_history.append({
                    "time":        datetime.datetime.now().strftime("%H:%M:%S"),
                    "script_name": sname,
                    "temps":       temps_str[:40] + ("…" if len(temps_str) > 40 else ""),
                    "steps":       ", ".join(step_labels[s] for s in steps),
                    "ok":          job_ok,
                    "job_id":      job_output.split("\n")[0][:60] if job_ok else "",
                    "detail":      "" if job_ok else ((job_stderr or job_output or "").strip()[:300]),
                })

    return _render(errors=errors, script_text=script_text,
                   script_path=script_path, qsub_cmd=qsub_cmd,
                   job_output=job_output, job_stderr=job_stderr, job_ok=job_ok,
                   pre_chmod_output=pre_chmod_output, pre_chmod_ok=pre_chmod_ok,
                   **fv)


@app.route("/clear_history", methods=["POST"])
def clear_history():
    with _history_lock:
        _job_history.clear()
    from flask import redirect
    return redirect("/")


if __name__ == "__main__":
    import datetime
    # threaded=True: the page fires continuous background polls (watch /
    # auto-watch / auto-submit / job-health). Without threading the dev server
    # serves only one request at a time, so a Browse (/browse_dir) request made
    # while a poll is in flight gets blocked and errors out — which looked like
    # "browse works once, then fails." Threading lets those overlap cleanly.
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)
