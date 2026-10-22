# Data inventory

## Traffic

- Source file: `raw/traffic/Metro_Interstate_Traffic_Volume.csv`
- Total rows: 48,204
- Columns: 9
- Full period: 2012-10-02 09:00:00 to 2018-09-30 23:00:00
- Selected simulation source period: 2013-03-01 to 2017-02-28
- Rows in selected period: 27,408
- Original ZIP SHA-256: `B99AEABCBD6CC86F642DA3A79D90883425798F58ABD3B3302DA2FA19DDA73768`

## Air quality

- Source files: `raw/air_quality/stations/PRSA_Data_20130301-20170228/*.csv`
- Stations: 12
- Rows per station: 35,064
- Total rows: 420,768
- Columns: 18
- Selected simulation source period: 2013-03-01 to 2017-02-28
- Rows in selected period: 420,768
- Original ZIP SHA-256: `B04DA438B2F331AC0FFD45AEBDFEC0D20D2367FEB5F6948C4B1F7CE1191E33C4`

## Accident

- Source files: `raw/accident/collisions_2022.csv` through `collisions_2025.csv`
- Total rows: 412,714
- Columns per file: 44
- Selected source and simulation period: 2022-01-01 to 2025-12-31
- 2022 rows: 106,004
- 2023 rows: 104,258
- 2024 rows: 100,927
- 2025 rows: 101,525
- 2022 SHA-256: `B09CDF3FC1B8163BB54EAFC221148F31C3C8BBBEB62A977DCB87147B77716D3D`
- 2023 SHA-256: `EB7809C6EB0C10D01603B918BE2BF8EFFAE39AFA4421F475013131827E8DCBA9`
- 2024 SHA-256: `DE63DF852B245F44F73A9B4E35662466DC2EA0DF269658B16E8C8721BBAAC88A`
- 2025 SHA-256: `633249567E95479A1C30B3F10B0A6271CED11684FB338D0B8B4054CA94B80AA6`

## Notes

- The `raw` files are immutable inputs.
- Traffic and air quality are mapped to 2022-01-01 through 2025-12-31.
- Accident dates already cover 2022-2025 and are not shifted.
- Traffic contains repeated timestamps associated with weather descriptions. Hourly
  aggregation should use an average or deterministic deduplication, not a sum.
- Accident categorical fields are coded values. Decode them in ETL using the official
  STATS19 data guide when human-readable labels are needed.
