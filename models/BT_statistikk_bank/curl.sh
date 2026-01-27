#!/bin/bash
curl -X PUT \
    -F manifest.json=@target/manifest.json \
    -F catalog.json=@target/catalog.json \
    -F index.html=@target/index.html \
    https://dbt.intern.nav.no/docs/familie/dvh_fam_bt