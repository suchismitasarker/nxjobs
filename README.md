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





