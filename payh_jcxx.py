from pprint import pprint
from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from payh_sbxx import handle
import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.abspath('Tools'))
import common
import datetime


class handle_jcxx():
    def __init__(self, nsrsbh, sssqz_max=None):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh = nsrsbh
        if sssqz_max:
            sssqz_max = datetime.datetime.strptime(sssqz_max, '%Y-%m-%d')
        self.sssqz_max = sssqz_max

    def transform_hydm(self, col):
        if col == 'C':
            return '1'
        elif col == 'A':
            return '2'
        elif col == '51':
            return '3'
        elif col == '52':
            return '4'
        else:
            return '5'

    def transform_sfz(self, col, nl=None):
        if nl:
            if len(col) == 18:
                return self.sssqz_max.year - int(col[6:10])
            elif len(col) == 15:
                bir = '19' + col[6:8]
                return self.sssqz_max.year - int(bir)
        else:
            if len(col) == 18:
                return int(col[17:18]) % 2
            elif len(col) == 15:
                return int(col[-1]) % 2

    def get_nsrjcxx_info(self):
        if not self.sssqz_max:
            data, self.sssqz_max = handle(self.nsrsbh,
                                          self.sssqz_max).get_sbxx_data()
        #nsr基础信息
        sql = '''select nsrsbh,sshydm ,zczb,nslxmc,xydj ,kyrq from
         (select * from t_nsrjcxx where nsrsbh='{0}' order by lrsj desc)
          where rownum=1'''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        nsr_df = common.dataframe(result, col)
        #行业代码
        sql = '''select hyml_dm,hymx_dm from dm_hy_2017'''
        result, col = self.db_handle.query(sql)
        hydm_df = common.dataframe(result, col)
        hydm_df.columns = ['hyml_dm', 'sshydm']
        nsr_info = pd.merge(nsr_df, hydm_df, how='left', on='sshydm')
        nsr_info['hy_class'] = nsr_info['hyml_dm'].apply(
            lambda x: self.transform_hydm(x))
        nsr_info['kyrq_dis'] = (self.sssqz_max - pd.to_datetime(
            nsr_info['kyrq'])).apply(lambda x: round(x.days / 365.0, 1))
        nsr_info.rename(
            columns={
                'sshydm': 'hy',
                'xydj': 'nsrxypj',
                'nslxmc': 'nsrlx'
            },
            inplace=True)
        nsr_info.drop(['hyml_dm', 'kyrq'], axis=1, inplace=True)
        #nsr 年龄与性别
        sql = '''select dbr_zjhm from zx_lxrxx_sample a where not exists
        (select 1 from zx_lxrxx_sample b where a.nsrsbh = b.nsrsbh
        and a.lrsj < b.lrsj - 3 / 24 / 60) and nsrsbh ='{0}'  
        and bssf = 1'''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        lxrxx_info = common.dataframe(result, col)
        nsr_info['nl_1'] = lxrxx_info['dbr_zjhm'].apply(
            lambda x: self.transform_sfz(x, True))
        nsr_info['xb_1'] = lxrxx_info['dbr_zjhm'].apply(
            lambda x: self.transform_sfz(x, False))
        nsr_info['dbr_zjhm'] = lxrxx_info['dbr_zjhm']
        #pprint(lxrxx_info.loc[0].ix[0][6:10])
        pprint(nsr_info)
        return nsr_info

    def get_tzf_info(self):
        #投资方信息
        sql = '''select distinct nsrsbh,tzfmc,tzfjjxzdm,tzbl,tzfjjxzmc,
            zjhm,tzbl * tzbl as tzbl2 from zx_tzfxx_sample a 
           where nsrsbh = '{0}' and not exists
           (select 1 from zx_tzfxx_sample b where a.nsrsbh = b.nsrsbh
            and a.lrsj < b.lrsj - 3 / 24 / 60)'''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        tzf_df = common.dataframe(result, col)
        tzf_info = DataFrame(index=[0])
        #tzfjjxzdm = ['400', '410', '411', '412', '413']
        tzf_info['holder_count'] = len(tzf_df)
        tzf_info['holder_count_natural'] = len(
            tzf_df[(tzf_df['tzfjjxzdm'].notnull())
                   & (tzf_df['tzfjjxzdm'].str.startswith('4'))])
        #tzf_info['holder_count_corporate'] = len(
        #    tzf_df[~(tzf_df['tzfjjxzdm'].str.startswith('4'))
        #           & (tzf_df['tzfjjxzdm'].notnull())])
        tzf_info['holder_count_corporate'] = tzf_info[
            'holder_count'] - tzf_info['holder_count_natural']
        tzf_info['hh_index'] = tzf_df['tzbl2'].sum()
        #第一大股东
        nsr_df = self.get_nsrjcxx_info()
        tzf_df_new = tzf_df[tzf_df['tzbl'] == tzf_df['tzbl'].max()]
        holder_first = tzf_df_new[(tzf_df_new['tzfjjxzdm'].str.startswith('4'))
                                  & (tzf_df_new['tzfjjxzdm'].notnull())]
        if len(holder_first) > 0:
            tzf_info['holder_first'] = 0
            tzf_info['holdr_first_frdb'] = 0
        else:
            tzf_info['holder_first'] = 1
        holdr_frdb_1 = tzf_df_new[
            (tzf_df_new['tzfjjxzdm'].str.startswith('4'))
            & (tzf_df_new['tzfjjxzdm'].notnull())
            & (tzf_df_new['zjhm'] == nsr_df['dbr_zjhm'].values[0])]
        holdr_frdb_2 = tzf_df_new[(tzf_df_new['tzfjjxzdm'].notnull())
                                  &
                                  (tzf_df_new['tzfjjxzdm'].str.startswith('4'))
                                  & (tzf_df_new['zjhm'] == 'X')]
        if len(holdr_frdb_1) > 0 or len(holdr_frdb_2) > 0:
            tzf_info['holder_first_frdb'] = 1
        elif 'holder_first_frdb' not in tzf_info.columns:
            tzf_info['holder_first_frdb'] = 2
        pprint(tzf_info)
        return tzf_info

    def get_bg_info(self):
        sql = '''select distinct bgrq, bgqnr, bghnr, bgxmmc
            from zx_bgdjxx_sample a where nsrsbh ='{0}' and bgqnr != bghnr
            and not exists
           (select 1
                    from zx_bgdjxx_sample b
                   where a.nsrsbh = b.nsrsbh
                     and substr(a.bgrq, 1, 4) = substr(b.bgrq, 1, 4)
                     and a.lrsj < b.lrsj - 3 / 24 / 60) '''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        bg_df = common.dataframe(result, col)
        pprint(bg_df)
        #变更名称
        bgmc = [
            '办税人员证件号码', '财务负责人身份证件号码', '生产经营地址', '投资方', '注册资本', '经营范围',
            ['法定代表人（负责人）移动电话', '法定代表人（负责人、业主）移动电话'], ['国标行业', '国标行业（附）']
        ]
        #指标名称代码
        zbmc = [
            'bg_bsry', 'bg_cwfzr', 'bg_dz', 'bg_tzf', 'bg_zczb', 'jyfw',
            'bg_frdbdh', 'bg_hybg'
        ]
        month_bins = [
            3,
            6,
            9,
            12,
            24,
        ]
        sssqz = self.sssqz_max
        bg_info = DataFrame(index=[0])
        flag = 0
        for i, j in zip(bgmc, zbmc):
            flag += 1
            for n in month_bins:
                #距离观察时间n个月
                date_diff_1 = (
                    sssqz - relativedelta(months=0)).strftime('%Y-%m-%d')
                date_diff_2 = (
                    sssqz - relativedelta(months=n)).strftime('%Y-%m-%d')
                #获取满足条件的数据
                if flag <= 6:
                    data = bg_df[(bg_df['bgxmmc'] == i)
                                 & (bg_df['bgrq'] < date_diff_1) &
                                 (bg_df['bgrq'] > date_diff_2)]
                else:
                    data = bg_df[(bg_df['bgxmmc'].isin(i))
                                 & (bg_df['bgrq'] < date_diff_1) &
                                 (bg_df['bgrq'] > date_diff_2)]
                bg_info[j + '_' + str(n) + 'm'] = len(data)
        pprint(bg_info)
        return bg_info

    def get_wfwz_info(self):
        sql = '''select distinct zywfwzss, djrq, wfwzlxdm
            from zx_wfwzxx_sample a where nsrsbh = '{0}' and not
            exists
            (select 1
                    from zx_wfwzxx_sample b
                    where a.nsrsbh = b.nsrsbh
                    and substr(a.DJRQ,1,4)=substr(b.DJRQ,1,4)
                    and a.lrsj < b.lrsj - 3 / 24 / 60) '''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        wfwz_df = common.dataframe(result, col)
        pprint(wfwz_df)
        #违章代碼
        wzmc = [
            '发票违法次数', '非主观故意违法次数', '抗税次数', '骗税次数', '其他违法次数', '税收政策例外违法次数',
            '税务机关执法不当次数', '逃避缴纳税款', '违反税收管理次数'
        ]
        wzdm = ['04', '06', '03', '02', '99', '08', '07', '01', '05', '']
        #指标名称
        zbmc = [
            'wfwz_fpwf', 'wfwz_fzggy', 'wfwz_ks', 'wfwz_ps', 'wfwz_qt',
            'wfwz_sszclwwfcs', 'wfwz_swjg', 'wfwz_tbjnsk', 'wfwz_wfssgl',
            'wfwz'
        ]
        month_bins = [3, 6, 9, 12]
        wfwz_info = DataFrame(index=[0])
        sssqz = self.sssqz_max
        for i, j in zip(wzdm, zbmc):
            for n in month_bins:
                #距离观察时间n个月
                date_diff_1 = (
                    sssqz - relativedelta(months=0)).strftime('%Y-%m-%d')
                date_diff_2 = (
                    sssqz - relativedelta(months=n)).strftime('%Y-%m-%d')
                #获取满足条件的数据
                if j == 'wfwz':
                    data = wfwz_df[(wfwz_df['djrq'] > date_diff_2)
                                   & (wfwz_df['djrq'] < date_diff_1)]
                else:
                    data = wfwz_df[(wfwz_df['wfwzlxdm'] == i)
                                   & (wfwz_df['djrq'] > date_diff_2) &
                                   ((wfwz_df['djrq'] < date_diff_1))]
                wfwz_info[j + '_' + str(n) + 'm'] = len(data)
        #wfwz_24m
        date_diff_1 = (sssqz - relativedelta(months=0)).strftime('%Y-%m-%d')
        date_diff_2 = (sssqz - relativedelta(months=24)).strftime('%Y-%m-%d')
        data = wfwz_df[(wfwz_df['djrq'] > date_diff_2)
                       & (wfwz_df['djrq'] < date_diff_1)]
        wfwz_info['wfwz_24m'] = len(data)

        pprint(wfwz_info)
        return wfwz_info

    def get_jc_info(self):
        #稽查信息
        sql = '''select distinct wfwzlxmc, aydjrq, jclxmc from
                    zx_jcajxx_sample a where nsrsbh = '{0}' and
                    not exists
                    (select 1
                        from zx_jcajxx_sample b
                        where a.nsrsbh = b.nsrsbh
                        and substr(a.AYDJRQ, 1, 4) = substr(b.AYDJRQ, 1, 4)
                        and a.lrsj < b.lrsj - 3 / 24 / 60) '''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        jc_df = common.dataframe(result, col)
        pprint(jc_df)
        month_bins = [3, 6, 12, 24]
        jc_info = DataFrame(index=[0])
        sssqz = self.sssqz_max
        for n in month_bins:
            #距离观察时间n个月
            date_diff_1 = (
                sssqz - relativedelta(months=0)).strftime('%Y-%m-%d')
            date_diff_2 = (
                sssqz - relativedelta(months=n)).strftime('%Y-%m-%d')
            #获取满足条件的数据
            data = jc_df[(jc_df['aydjrq'] < date_diff_1)
                         & (jc_df['aydjrq'] > date_diff_2)]
            jc_info['jcaj' + '_' + str(n)] = len(data)
        pprint(jc_info)
        return jc_info

    def get_concat_df(self):
        try:
            nsr_info = self.get_nsrjcxx_info()
        except:
            basic_info = DataFrame(index=[0])
            basic_info['nsrsbh'] = self.nsrsbh
            basic_info['remark'] = '申报缺失'
            return basic_info
        nsr_info.drop('dbr_zjhm', axis=1, inplace=True)
        tzf_info = self.get_tzf_info()
        bg_info = self.get_bg_info()
        wz_info = self.get_wfwz_info()
        jc_info = self.get_jc_info()
        df_list = [nsr_info, tzf_info, bg_info, wz_info, jc_info]
        basic_info = pd.concat(df_list, axis=1)
        basic_info['remark'] = None
        pprint(basic_info)
        return basic_info


if __name__ == "__main__":
    h = handle_jcxx('91350211594998858T')
    h.get_nsrjcxx_info()
    #h.get_tzf_info('445381584686367')
    #h.get_bg_info('32020071744299X')
    #h.get_wfwz_info('32020071744299X')
    #h.get_jc_info('32020071744299X')
    #concat_df = h.get_concat_df()
    #pprint(concat_df[[
    #    'holder_count_corporate', 'holder_first', 'holder_first_frdb'
    #]])
