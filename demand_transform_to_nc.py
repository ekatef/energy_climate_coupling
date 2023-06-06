import numpy as np
import pandas as pd
import xarray as xr

# load nc should have the following structure ----------------------------------
# Dimensions:             (time: 8760, region_code: 50)
# Coordinates:
#   * time                (time) datetime64[ns] 2013-01-01 ... 2013-12-31T23:00:00
#     region_name         (region_code) object dask.array<chunksize=(50,), meta=np.ndarray>
#   * region_code         (region_code) object 'AE' 'AF' 'AM' ... 'UZ' 'VN' 'YE'
# Data variables:
#     Electricity demand  (region_code, time) float64 dask.array<chunksize=(50, 8760), meta=np.ndarray>



# transform csv to nc ----------------------------------------------------------
csv_fl = "~/pypsa-earth/data/ssp2-2.6/2030/era5_2013_IR2/Asia.csv"

# df = pd.read_csv(csv_fl)
df = pd.read_csv(csv_fl, sep = ";")
df.time = pd.to_datetime(df.time, format="%Y-%m-%d %H:%M:%S")

regions = {c:n for c, n in zip(df.region_code, df.region_name)}

df.time = pd.to_datetime(df.time, format="%Y-%m-%d %H:%M:%S")
xds = df.set_index(["time", "region_code"]).to_xarray()
xds = xds.assign_coords(
    {"region_name":("region_code", [name for (code, name) in regions])}
)

xds.to_netcdf("~/pypsa-earth/data/ssp2-2.6/2030/era5_2013_IR2/Asia.nc")
