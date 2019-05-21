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


class handle_zsxx():
    def __init__(self, nsrsbh, sssqz_max=None):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh = nsrsbh
        if sssqz_max:
            sssqz_max = datetime.datetime.strptime(sssqz_max, '%Y-%m-%d')
        self.sssqz_max = sssqz_max
        self.zsxx_info = None

    def get_zsxx_info(self):
        sssqz_max = self.sssqz_max.strftime('%Y-%m-%d')
        data, sssqz_max = handle(self.nsrsbh, sssqz_max).get_sbxx_data()
        if not self.sssqz_max:
            self.sssqz_max = sssqz_max
        sssqz = sssqz_max.strftime('%Y-%m-%d')
        sql = '''select distinct nsrsbh,
                    sssq_q,
                    sssq_z,
                    jkqx,
                    jkfsrq,
                    se,
                    zsxm_mc,
                    SKZL_MC
                from zx_sbzsxx_sample_w_2 a
                where not exists (select 1
                from zx_sbzsxx_sample_w_2 b
                where a.nsrsbh = b.nsrsbh
                and substr(a.sssq_q,1,4)=substr(b.sssq_q,1,4)
                and a.lrsj < b.lrsj - 3 / 24 / 60)
         and sssq_z <= '{0}'
         and nsrsbh = '{1}' '''.format(sssqz, self.nsrsbh)
        result, col = self.db_handle.query(sql)
        zsxx_info = common.dataframe(result, col)
        #self.sssqz_max = datetime.datetime.strptime(zsxx_info['sssq_z'].max(),
        #                                            '%Y-%m-%d')
        self.zsxx_info = zsxx_info
        print(zsxx_info.head())

    def get_znj_info(self):
        data = self.zsxx_info
        znj_info = DataFrame(index=[0])
        '''欠税状态'''
        date_diff = datetime.datetime.strftime(self.sssqz_max, '%Y-%m-%d')
        df = data[(data['sssq_z'] == date_diff) & (data['jkfsrq'].isnull()) &
                  (data['jkfsrq'] == 'NULL') & (data['se'] > 0)]
        znj_info['qs_1'] = len(df)

        months_bin = [i for i in range(1, 13)]
        months_bin.append(24)
        for i in months_bin:
            '''距sssqz_max i个月'''
            date_diff = (
                self.sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            '''滞纳金'''
            znj = data[(data['skzl_mc'] == '滞纳金')
                       & (data['sssq_z'] > date_diff)]
            znj_info['znj_' + str(i)] = znj['se'].sum()
            '''缴纳延迟次数'''
            pay_delay = data[(data['sssq_z'] > date_diff)
                             & (data['jkfsrq'] > data['jkqx'])]
            znj_info['pay_delay_' + str(i) + 'm'] = len(pay_delay)
            '''缴纳延迟次数(增值税)'''
            znj_info['pay_delay_zzs_' + str(i) + 'm'] = len(
                pay_delay[pay_delay['zsxm_mc'] == '增值税'])
            '''缴纳延迟次数(企业所得税)'''
            znj_info['pay_delay_sds_' + str(i) + 'm'] = len(
                pay_delay[pay_delay['zsxm_mc'] == '企业所得税'])
            '''缴纳延迟次数（3天以上）'''
            data['jkqx_dis'] = (
                pd.to_datetime(data['jkfsrq']) - pd.to_datetime(
                    data['jkqx'])).apply(lambda x: x.days)
            pay_delay_3d = data[(data['sssq_z'] > date_diff)
                                & (data['jkqx_dis'] > 3)]
            znj_info['pay_delay_' + str(i) + 'm_3d'] = len(pay_delay_3d)
            '''缴纳延迟次数（增值税）（3天以上）'''
            znj_info['pay_delay_zzs_' + str(i) + 'm_3d'] = len(
                pay_delay_3d[pay_delay_3d['zsxm_mc'] == '增值税'])
            znj_info['pay_delay_sds_' + str(i) + 'm_3d'] = len(
                pay_delay_3d[pay_delay_3d['zsxm_mc'] == '企业所得税'])

        pprint(znj_info.head())
        return znj_info

    def get_nse_info(self):
        data = self.zsxx_info
        nse_info = DataFrame(index=[0])
        months_bin = [i for i in range(1, 13)]
        months_bin.append(24)
        for i in months_bin:
            '''距sssqz_max i个月'''
            date_diff = (
                self.sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            '''纳税金额（增值税）'''
            sum_pay_zzs = data[(data['sssq_z'] > date_diff)
                               & (data['zsxm_mc'] == '增值税')]
            nse_info['sum_pay_zzs_' + str(i) + 'm'] = sum_pay_zzs['se'].sum()
            '''纳税金额（企业所得税）'''
            sum_pay_sds = data[(data['sssq_z'] > date_diff)
                               & (data['zsxm_mc'] == '企业所得税')]
            nse_info['sum_pay_sds_' + str(i) + 'm'] = sum_pay_sds['se'].sum()
            '''纳税金额'''
            nse_info['sum_pay_' + str(i) +
                     'm'] = sum_pay_zzs['se'].sum() + sum_pay_sds['se'].sum()
        print(nse_info.head())
        return nse_info

    def get_behavior_info(self):
        data = self.zsxx_info
        pprint(data)
        behavior_info = DataFrame(index=[0])
        months_bin = [i for i in range(1, 13)]
        months_bin.append(24)
        for i in months_bin:
            '''距sssqz_max i个月'''
            date_diff = (
                self.sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            behavior_fine = data[(data['skzl_mc'] == '行为罚款')
                                 & (data['sssq_z'] > date_diff)]
            '''行为罚款次数'''
            behavior_info['behavior_fine_' + str(i)] = len(behavior_fine)
            '''行为罚款金额'''
            behavior_info['sum_behavior_fine_' +
                          str(i)] = behavior_fine['se'].sum()
        pprint(behavior_info.head())
        return behavior_info

    def get_pay_ratio(self):
        data = self.zsxx_info
        nse_info = self.get_nse_info()
        pay_ratio_info = DataFrame(index=[0])
        month_bin_1 = [3, 6, 9, 12]
        month_bin_2 = [6, 12, 18, 24]
        for i, j in zip(month_bin_1, month_bin_2):
            date_diff_1 = (
                self.sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            date_diff_2 = (
                self.sssqz_max - relativedelta(months=j)).strftime('%Y-%m-%d')
            '''增值税'''
            zzs_df = data[(data['zsxm_mc'] == '增值税')
                          & (data['sssq_z'] <= date_diff_1) &
                          (data['sssq_z'] > date_diff_2)]
            '''企业所得税'''
            sds_df = data[(data['zsxm_mc'] == '企业所得税')
                          & (data['sssq_z'] <= date_diff_1) &
                          (data['sssq_z'] > date_diff_2)]
            '''近i个月纳税额增长率(增值税)'''
            pay_ratio_info['sum_pay_zzs_gth_ratio_' + str(i)] = np.divide(
                (nse_info['sum_pay_zzs_' + str(i) + 'm'] - zzs_df['se'].sum()),
                zzs_df['se'].sum())
            '''近i个月纳税额增长率(企业所得税)'''
            pay_ratio_info['sum_pay_sds_gth_ratio_' + str(i)] = np.divide(
                (nse_info['sum_pay_sds_' + str(i) + 'm'] - sds_df['se'].sum()),
                sds_df['se'].sum())
            '''近i个月纳税额增长率'''
            nsze = zzs_df['se'].sum() + sds_df['se'].sum()
            pay_ratio_info['sum_pay_gth_ratio_' + str(i)] = np.divide(
                nse_info['sum_pay_' + str(i) + 'm'] - nsze, nsze)
        pprint(pay_ratio_info)
        return pay_ratio_info

    def sort_by_col(self, df, n):
        df_new = df.sort_values(by='sssq_z', ascending=False)
        df_new.index = range(len(df))
        return df_new.loc[:n]

    def get_zs_count(self):
        data = self.zsxx_info
        zs_count_info = DataFrame(index=[0])
        for i in range(24):
            zs = self.sort_by_col(data, i)
            '''最近i期征收不为0的次数'''
            zs_count_info['zs_zero_count_' + str(i + 1)] = len(
                zs[zs['se'] != 0])
            '''最近i期增值税征收不为0的次数'''
            zs_zzs = data[data['zsxm_mc'] == '增值税']
            zs_zzs = self.sort_by_col(zs_zzs, i)
            zs_count_info['zs_zzs_zero_count_' + str(i + 1)] = len(
                zs_zzs[zs_zzs['se'] != 0])
            '''最近i期企业所得税征收不为0的次数'''
            zs_sds = data[data['zsxm_mc'] == '企业所得税']
            zs_sds = self.sort_by_col(zs_sds, i)
            zs_count_info['zs_sds_zero_count_' + str(i + 1)] = len(
                zs_sds[zs_sds['se'] != 0])
        pprint(zs_count_info.head())
        return zs_count_info

    def get_sbzs_info(self):
        sssqz_max = self.sssqz_max.strftime('%Y-%m-%d')
        data, sssqz = handle(self.nsrsbh, sssqz_max).get_sbxx_data()
        xse_info = handle(self.nsrsbh, sssqz_max).get_xse_info()
        nse_info = self.get_nse_info()
        behavior_info = self.get_behavior_info()
        behavior_and_tax_ratio_info = DataFrame(index=[0])
        month_bin_1 = [3, 6, 9, 12, 24]
        month_bin_2 = [m for m in range(1, 13)]
        month_bin_2.append(24)
        for i in month_bin_1:
            '''税负率'''
            behavior_and_tax_ratio_info.loc[:, 'tax_bearing_ratio_' +
                                            str(i)] = np.divide(
                                                nse_info['sum_pay_' + str(i) +
                                                         'm'],
                                                xse_info['qbxse_' + str(i)])
            if i == 12:
                for j in month_bin_2:
                    behavior_and_tax_ratio_info.loc[:, 'ratio_behavior_fine_' +
                                                    str(j)] = np.divide(
                                                        behavior_info[
                                                            'sum_behavior_fine_'
                                                            + str(j)],
                                                        xse_info['qbxse_' +
                                                                 str(i)])
        pprint(behavior_and_tax_ratio_info)
        return behavior_and_tax_ratio_info

    def get_concat_df(self):
        try:
            zsxx_info = self.get_zsxx_info()
        except:
            zsxx_info = DataFrame(index=[0])
            zsxx_info['nsrsbh'] = self.nsrsbh
            zsxx_info['remark'] = '申报缺失'
            return zsxx_info
        znj_info = self.get_znj_info()
        nse_info = self.get_nse_info()
        pay_ratio = self.get_pay_ratio()
        zs_count = self.get_zs_count()
        sbzs_info = self.get_sbzs_info()
        behavior_info = self.get_behavior_info()
        col = [
            zsxx_info, znj_info, nse_info, pay_ratio, sbzs_info, behavior_info,
            zs_count
        ]
        sbzs_feature = pd.concat(col, axis=1)
        sbzs_feature['nsrsbh'] = self.nsrsbh
        sbzs_feature['sssq'] = self.sssqz_max
        pprint(sbzs_feature.head())
        return sbzs_feature


if __name__ == '__main__':
    h = handle_zsxx('91350211594998858T', '2017-03-01')
    h.get_zsxx_info()
    #h.get_znj_info()
    #h.get_nse_info()
    #h.get_pay_ratio()
    h.get_zs_count()
    #h.get_sbzs_info('')
    #h.get_behavior_info()
    #sbzs_feature = h.get_concat_df()
    #pprint(sbzs_feature)