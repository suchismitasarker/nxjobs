# nxjobs — NxRefine / NxReduce Job Submission

A small Flask web app (http://localhost:5050) for the CHESS **QM2** beamline
(ID4B). It generates per-temperature `nxreduce` / `nxfind` processing scripts
and submits them to the CLASSE SGE cluster (`lnx201`) with `qsub`.

This repo contains the app and an Apptainer (Singularity) deployment so it can
be run as a container on `lnx201`.

```
nxjobs/
├── nxjobs-app.py                      # the app (single file)
├── deploy/
│   └── apptainer/
│       ├── nxjobs.def                 # container recipe
│       ├── build.sh                   # build nxjobs.sif
│       ├── run.sh                     # launch the app (handles SSH submission)
│       └── nxjobs_ssh.env.example     # template for SSH submission settings
├── .gitignore
└── README.md
```

## Build

On `lnx201` (Apptainer must be on PATH):

```bash
cd nxjobs
deploy/apptainer/build.sh          # produces deploy/apptainer/nxjobs.sif
```

Rebuild any time you change `nxjobs-app.py` — the code is baked into the image.

## Run

```bash
deploy/apptainer/run.sh            # http://localhost:5050
```

By default the app submits jobs **locally** (it expects `qsub` and the shared
filesystem to be reachable on the node it runs on).

## Submitting jobs over SSH (for containerized / off-host use)

A container has no SGE and no NFS group-write permission, so to submit jobs it
logs in over SSH to `lnx201` **as a real user** and runs `qsub` there.

In this build, SSH submission is configured **in the web UI**: the submit form
has Remote host / Remote user / Remote key fields (handled by `_run_qsub` in the
app). Fill them in and the app submits over SSH for you. Defaults: host
`lnx201.classe.cornell.edu`, user = your login.

One-time per user, so there's no password/Duo prompt on every submit:

```bash
ssh-keygen -t ed25519                              # if you have no key yet
ssh-copy-id <yourid>@lnx201.classe.cornell.edu     # one Duo login
```

Then point the app's Remote key field at `~/.ssh/id_ed25519` (or leave the
app's agent / default-key lookup to find it).

> Note: `deploy/apptainer/run.sh` can also preseed `NXJOBS_SSH_*` env vars or
> load a `nxjobs_ssh.env` config (see `nxjobs_ssh.env.example`) — a convenience
> for baking defaults into the container. This version of the app ultimately
> reads its SSH settings from the submit form.



