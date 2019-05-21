from pprint import pprint
from pandas import DataFrame
from dateutil.relativedelta import relativedelta
import os
import sys
sys.path.append(os.path.abspath('Tools'))
import pandas as pd
import numpy as np
import common
import datetime


class handle():
    def __init__(self, nsrsbh, sssqz_max=None):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh = nsrsbh
        self.data = None
        if sssqz_max:
            sssqz_max = datetime.datetime.strptime(sssqz_max, '%Y-%m-%d')
        self.sssqz_max = sssqz_max

    def get_sbxx_data(self):
        sql = '''select distinct nsrsbh,
                    t.sssqq,
                    t.sssqz,
                    t.qbxse,
                    t.ysxssr,
                    t.ybtse,
                    t.yjse,
                    t.jmse,
                    t.sbrq,
                    t.sbqx,
                    t.zsxmmc
        from  zx_sbxx_sample_w_2 t
        where t.nsrsbh = '{0}'
        and sssqq >= to_char(sysdate - 365 * 3, 'yyyy-mm-dd')
        and ZSXMMC IN ('增值税', '企业所得税')
        and not exists (select 1
                from zx_sbxx_sample_w_2 b
                where t.nsrsbh = b.nsrsbh
                and t.lrsj < b.lrsj - 3 / 24 / 60)'''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        sbxx_df = common.dataframe(result, col)
        sssqz_max = datetime.datetime.strptime(sbxx_df['sssqz'].max(),
                                               '%Y-%m-%d')
        if not self.sssqz_max:
            self.sssqz_max = sssqz_max
        for i in ['qbxse', 'jmse', 'yjse', 'ybtse']:
            sbxx_df[i] = sbxx_df[i].astype('float')
        sssqz = self.sssqz_max.strftime('%Y-%m-%d')
        self.data = sbxx_df[sbxx_df['sssqz'] <= sssqz]
        pprint(sbxx_df)
        return self.data, self.sssqz_max

    def get_sb_info(self):
        sb_df = self.data[['nsrsbh', 'sbrq', 'sbqx', 'sssqz']]
        sb_info = DataFrame(index=[0])
        sb_info['nsrsbh'] = self.nsrsbh
        sb_info['sssqz_max'] = sb_df['sssqz'].max()
        #距离观察时间近12个月的时间
        date_diff = (
            self.sssqz_max - relativedelta(months=12)).strftime('%Y-%m-%d')
        data = sb_df[sb_df['sbrq'] > date_diff]
        pprint(data)
        data['sbrq_diff'] = (pd.to_datetime(data['sbqx']) - pd.to_datetime(
            data['sbrq'])).apply(lambda x: x.days)
        sb_info['sbrq_jz'] = data['sbrq_diff'].mean()
        sb_info['sbrq_fc'] = data['sbrq_diff'].var()
        print(sb_info)
        return sb_info

    def get_tax_cumsum(self):
        '''增值税，企业所得税累计'''
        data = self.data[['sssqz', 'yjse', 'ybtse', 'zsxmmc']]
        month_bins = [(0, 3), (0, 6), (0, 9), (0, 12), (12, 15), (12, 18),
                      (12, 21), (12, 24)]
        col_bins = ['3', '6', '9', '12', '1_3', '1_6', '1_9', '24']
        tax_col = ['zzs', 'qysds']
        tax_name = ['增值税', '企业所得税']
        tax_cumsum_info = DataFrame(index=[0])
        for m, n in enumerate(tax_name):
            for i, j in enumerate(month_bins):
                date_diff_1 = (self.sssqz_max -
                               relativedelta(months=j[0])).strftime('%Y-%m-%d')
                date_diff_2 = (self.sssqz_max -
                               relativedelta(months=j[1])).strftime('%Y-%m-%d')
                df = data[(data['sssqz'] <= date_diff_1)
                          & (data['sssqz'] > date_diff_2) &
                          (data['zsxmmc'] == n)]
                #增值税
                if tax_col[m] == 'zzs':
                    tax_cumsum_info[
                        tax_col[m] + '_' +
                        col_bins[i]] = df['ybtse'].sum() + df['yjse'].sum()
                #企业所得税
                else:
                    tax_cumsum_info[tax_col[m] + '_' +
                                    col_bins[i]] = df['ybtse'].sum()
        pprint(tax_cumsum_info.head())
        return tax_cumsum_info

    def get_sb_count(self):
        sssqz_max = self.sssqz_max.strftime('%Y-%m-%d')
        data = self.data[['sssqz', 'qbxse', 'zsxmmc', 'sbrq', 'sbqx']]
        sssqz_min = datetime.datetime.strptime(data['sssqz'].min(), '%Y-%m-%d')
        tax_count_info = DataFrame(index=[0])
        month_bins = [3, 6, 9, 12]
        for i in month_bins:
            date_diff = (
                self.sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            #近i个月0申报次数
            data_sb = data[(data['zsxmmc'] == '增值税')
                           & (data['sssqz'] > date_diff)
                           & (data['sssqz'] <= sssqz_max)
                           & (data['qbxse'] == 0)]
            tax_count_info['sb_0' + str(i)] = len(data)
        '''最近一期申报是否按时'''
        yqsb_0 = data[(data['sssqz'] == sssqz_max)
                      & (data['sbrq'] > data['sbqx'])
                      & (data['zsxmmc'] == '增值税')]
        tax_count_info['yqsb_01'] = len(yqsb_0)
        '''近j个月逾期申报次数'''
        for j in range(2, 25):
            date_diff = (
                self.sssqz_max - relativedelta(months=j)).strftime('%Y-%m-%d')
            yqsb = data[(data['sssqz'] > date_diff)
                        & (data['zsxmmc'] == '增值税')
                        & (data['sbrq'] > data['sbqx'])]
            if j < 10:
                tax_count_info['yqsb_0' + str(j)] = len(yqsb)
            else:
                tax_count_info['yqsb' + str(j)] = len(yqsb)
        '''最近m期申报为0的次数'''
        sb_zero_count = data[data['zsxmmc'] == '增值税']
        sb_zero_count = sb_zero_count.sort_values(by='sssqz', ascending=False)
        sb_zero_count.index = range(1, len(sb_zero_count) + 1)
        for m in range(1, 25):
            df = sb_zero_count.iloc[:m, :]
            zero_count = df[df['qbxse'] == 0]
            tax_count_info['sb_zero_count_' + str(m)] = len(zero_count)
        '''最近n期连续申报为0的最大次数'''
        for n in range(2, 25):
            df = sb_zero_count.iloc[:n, :]
            index = df[df['qbxse'] == 0].index
            tax_count_info['sb_zero_continuous_' +
                           str(n)] = common.max_sequence_count(index)
        '''最近三年，最远的申报距今月份数'''
        sbrq_first = int((self.sssqz_max - sssqz_min).days / 30)
        tax_count_info['sbrq_first'] = sbrq_first
        print(tax_count_info)
        return tax_count_info

    def get_xse_info(self):
        data, sssqz_max = self.get_sbxx_data()
        xse_info = DataFrame(index=[0])
        now_year = sssqz_max.year
        last_year = now_year - 1
        '''当年销售额累计'''
        data = data[['qbxse', 'sssqz', 'zsxmmc']]
        data.loc[:, 'year'] = data['sssqz'].apply(lambda x: int(x[:4]))
        qbxse_last = data[(data['zsxmmc'] == '增值税')
                          & (data['year'] == now_year)]
        xse_info.loc[:, 'qbxse_last'] = qbxse_last['qbxse'].sum()
        '''上年销售额累计(t-1年截止到t-1年当前月累计)'''
        sssqz_last = (sssqz_max - relativedelta(years=1)).strftime('%Y-%m-%d')
        qbxse_pre = data[(data['zsxmmc'] == '增值税')
                         & (data['year'] == last_year)
                         & (data['sssqz'] <= sssqz_last)]
        xse_info.loc[:, 'qbxse_pre'] = qbxse_pre['qbxse'].sum()
        '''上年全部销售额累计'''
        qbxse_1_1 = data[(data['zsxmmc'] == '增值税')
                         & (data['year'] == last_year)]
        xse_info.loc[:, 'qbxse_1_1'] = qbxse_1_1['qbxse'].sum()
        '''当年累计纳税额增长率'''
        xse_info.loc[:, 'nseZzl_lj'] = np.divide(
            xse_info['qbxse_last'] - xse_info['qbxse_pre'],
            xse_info['qbxse_pre'])
        for i in range(1, 25):
            date_diff_1 = (
                sssqz_max - relativedelta(months=0)).strftime('%Y-%m-%d')
            date_diff_2 = (
                sssqz_max - relativedelta(months=i)).strftime('%Y-%m-%d')
            qbxse = data[(data['zsxmmc'] == '增值税')
                         & (data['sssqz'] <= date_diff_1)
                         & (data['sssqz'] > date_diff_2)]
            '''销售额近i月累计'''
            xse_info.loc[:, 'qbxse_' + str(i)] = qbxse['qbxse'].sum()
            '''变异系数'''
            if i >= 2:
                xse_info.loc[:, 'cv_' + str(i)] = 100 * np.divide(
                    qbxse['qbxse'].std(), qbxse['qbxse'].mean())
        '''销售额近13-24月离散系数'''
        date_diff_1 = (
            sssqz_max - relativedelta(months=12)).strftime('%Y-%m-%d')
        date_diff_2 = (
            sssqz_max - relativedelta(months=24)).strftime('%Y-%m-%d')
        lsxs_24 = data[(data['zsxmmc'] == '增值税')
                       & (data['sssqz'] <= date_diff_1)
                       & (data['sssqz'] > date_diff_2)]
        xse_info.loc[:, 'lsxs_24'] = 100 * np.divide(lsxs_24['qbxse'].std(),
                                                     lsxs_24['qbxse'].mean())
        '''销售额上年同期j个月累计'''
        for j in range(1, 13):
            date_diff_1 = (
                sssqz_max - relativedelta(months=12)).strftime('%Y-%m-%d')
            date_diff_2 = (
                sssqz_max - relativedelta(months=12 + j)).strftime('%Y-%m-%d')
            qbxse_pre = data[(data['zsxmmc'] == '增值税')
                             & (data['sssqz'] > date_diff_2) &
                             (data['sssqz'] <= date_diff_1)]
            xse_info.loc[:, 'qbxse_pre_' + str(j)] = qbxse_pre['qbxse'].sum()
        '''销售额上年同期m个月增长率'''
        for m in range(1, 13):
            sub = xse_info['qbxse_pre_' + str(m)] - xse_info['qbxse_' + str(m)]
            xse_info.loc[:, 'qbxse_zzl_' + str(m)] = np.divide(
                sub, xse_info['qbxse_' + str(m)])
        #print(xse_info)
        return xse_info

    def get_xse_ratio(self):
        xse_info = self.get_xse_info()
        '''销售收入增长率，纳税额增长率，上年同期增长率'''
        data = self.data[['sssqz', 'qbxse', 'yjse', 'ybtse', 'zsxmmc']]
        month_bins = [3, 6, 9, 12]
        xse_ratio_info = DataFrame(index=[0])
        xse_ratio_info['Zzl_lj'] = np.divide(
            xse_info['qbxse_last'] - xse_info['qbxse_pre'],
            xse_info['qbxse_pre'])
        for i in month_bins:
            #近i个月销售比增长率
            sub = xse_info['qbxse_' + str(i)] - xse_info['qbxse_pre_' + str(i)]
            xse_ratio_info['Zzl_' + str(i)] = np.divide(
                sub, xse_info['qbxse_pre_' + str(i)])
        pprint(xse_ratio_info)
        return xse_ratio_info

    def get_concat_df(self):
        try:
            data, sssqz = self.get_sbxx_data()
        except:
            sb_feature = DataFrame(index=[0])
            sb_feature['nsrsbh'] = self.nsrsbh
            sb_feature['remark'] = '申报缺失'
            pprint(sb_feature)
            return sb_feature
        tax_cumsum_info = self.get_tax_cumsum()
        sb_info = self.get_sb_info()
        sb_count = self.get_sb_count()
        xse_info = self.get_xse_info(data, sssqz)
        xse_ratio_info = self.get_xse_ratio(data, sssqz)
        col = [tax_cumsum_info, sb_info, sb_count, xse_info, xse_ratio_info]
        sb_feature = pd.concat(col, axis=1)
        sb_feature['remark'] = None
        pprint(sb_feature)
        return sb_feature


if __name__ == '__main__':
    h = handle('91350211594998858T', '2017-03-31')
    data, sssqz_max = h.get_sbxx_data()
    #h.get_tax_cumsum()
    #h.get_sb_info()
    #h.get_sb_count()
    xse_info = h.get_xse_info()
    pprint(xse_info)
    #h.get_concat_df()
