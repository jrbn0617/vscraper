import numpy as np
import pandas as pd

measure_size = 3
cut_ratio = 0.5
asset_size = 100
cut_rank = int(asset_size * cut_ratio)

values = np.random.random_sample((asset_size, measure_size))

df = pd.DataFrame(columns=range(measure_size), data=values)
rank_df = df.rank(axis=0)
mask_df = rank_df > cut_rank
mask_sr = mask_df.apply(all, axis=1)

good_asset_rank_df = rank_df[mask_sr]
good_asset_df = df[mask_sr]

#
# def cut_rank(price_df, vol_df, pbr_df, roe_df, index_date):
#     vol = low_sorting(price_df, vol_df, index_date, None)  # 해당일자의 저vol주 소팅
#     vol['vol순위'] = vol.rank()
#
#     pbr = low_sorting(price_df, pbr_df, index_date, None)  # 해당일자의 저pbr주 소팅
#     pbr['pbr순위'] = pbr.rank()
#
#     roe = low_sorting(price_df, roe_df, index_date, None)  # 해당일자의 고roe주 소팅
#     roe['roe순위'] = roe.rank(ascending=False)
#
#     rank_df = pd.concat([vol['vol순위'], pbr['pbr순위'], roe['roe순위']], axis=1)
#
#     cut_ratio = 0.5
#     cut_rank = int(len(rank_df) * cut_ratio)
#
#     mask_df = rank_df > cut_rank
#     mask_sr = mask_df.apply(all, axis=1)
#
#     return rank_df[mask_sr]