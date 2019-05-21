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


class handle_sxyxx():
    def __init__(self, nsrsbh, sssqz_max=None):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh = nsrsbh
        if sssqz_max:
            sssqz_max = datetime.datetime.strptime(sssqz_max, '%Y-%m-%d')
        self.sssqz_max = sssqz_max
        self.sxy = None
        self.now_year = None
        self.last_year = None
        self.qbxse_last = None
        self.qbxse_pre = None

    def get_sxy_data(self):
        #data, sssqz_max = handle(self.nsrsbh, self.sssqz_max).get_sbxx_data()
        sssqz_max = self.sssqz_max.strftime('%Y-%m-%d')
        xse_info = handle(self.nsrsbh, sssqz_max).get_xse_info()
        self.qbxse_last = xse_info['qbxse_last'].min()
        self.qbxse_pre = xse_info['qbxse_1_1'].min()
        if not self.sssqz_max:
            self.now_year = sssqz_max.strftime('%Y-%m-%d')[:4]
        else:
            self.now_year = self.sssqz_max.strftime('%Y-%m-%d')[:4]
        self.last_year = str(int(self.now_year) - 1)
        sql = '''select distinct jyje,
                        jyjebl,
                        gfnsrsbh,
                        xfnsrsbh,
                        sssq,
                        sxybz,
                        nsrsbh,
                        se,
                        pm,
                        nsrmc
                from zx_jydx a
                where nsrsbh = '{0}'
                and sssq <= '{1}'
                and not exists
                (select *
                    from zx_jydx b
                    where a.nsrsbh = b.nsrsbh
                    and a.sssq = b.sssq
                    and a.lrsj < b.lrsj - 3 / 24 / 60)'''.format(
            self.nsrsbh, self.now_year)
        result, col = self.db_handle.query(sql)
        self.sxy = common.dataframe(result, col)
        for col in ['jyje', 'jyjebl', 'se', 'pm', 'sxybz']:
            self.sxy[col] = self.sxy[col].astype('float')

        pprint(self.sxy)

    def get_sxy_xse_info(self):
        data = self.sxy
        sxy_xse_info = DataFrame(index=[0])
        years = [self.now_year, self.last_year]
        sxybz = [1, 0]
        sxymc = ['xy_', 'sy_']
        for year in years:
            for bz, mc in zip(sxybz, sxymc):
                for i in range(1, 11):
                    if year == self.now_year:
                        xse_df = data[(data['sxybz'] == bz) & (data['pm'] == i)
                                      & (data['sssq'] == year)]
                        #当前年上、下游销售额
                        sxy_xse_info.loc[:, mc + str(i)] = xse_df['jyje'].max()
                        #当前年上、下游比例
                        sxy_xse_info.loc[:, mc + 'bl_' + mc +
                                         str(i)] = xse_df['jyjebl'].max()
                        if i >= 2:
                            xse_hj = data[(data['sxybz'] == bz)
                                          & (data['pm'] <= i) &
                                          (data['sssq'] == year)]
                            #销售额合计
                            sxy_xse_info.loc[:, mc + 'hj_' +
                                             str(i)] = xse_df['jyje'].sum()
                            #比例合计
                            sxy_xse_info.loc[:, mc + 'hj_bl_' + mc +
                                             str(i)] = xse_hj['jyjebl'].sum()

                        if i == 10:
                            #企业个数
                            sxy_xse_info.loc[:, mc + 'count'] = len(
                                data[(data['sxybz'] == bz) & (data['pm'] <= i)
                                     & (data['sssq'] == year)])
                    if year == self.last_year:
                        xse_df = data[(data['sxybz'] == bz) & (data['pm'] == i)
                                      & (data['sssq'] == year)]
                        #去年上、下游销售额
                        sxy_xse_info.loc[:, mc + str(i) +
                                         '_pre'] = xse_df['jyje'].max()
                        #当前年上、下游比例
                        sxy_xse_info.loc[:, mc + 'bl_' + mc + str(i) +
                                         '_pre'] = xse_df['jyjebl'].max()
                        if i >= 2:
                            xse_hj = data[(data['sxybz'] == bz)
                                          & (data['pm'] <= i) &
                                          (data['sssq'] == year)]
                            #销售额合计
                            sxy_xse_info.loc[:, mc + 'hj_' + str(i) +
                                             '_pre'] = xse_hj['jyje'].sum()
                            #比例合计
                            sxy_xse_info.loc[:, mc + 'hj_bl_' + mc + str(i) +
                                             '_pre'] = xse_df['jyjebl'].sum()
                        if i == 10:
                            #企业个数
                            sxy_xse_info.loc[:, mc + 'count_pre'] = len(
                                data[(data['sxybz'] == bz) & (data['pm'] <= i)
                                     & (data['sssq'] == year)])
                #金额合计
                if year == self.now_year:
                    jehj = data[(data['sssq'] == year) & (data['pm'] == 12) &
                                (data['sxybz'] == bz)]
                    sxy_xse_info[mc + 'hj_bn'] = jehj['jyje'].max()
                    sxy_xse_info[mc + 'ze'] = jehj['jyje'].max()
                if year == self.last_year:
                    jehj = data[(data['sssq'] == year) & (data['pm'] == 12) &
                                (data['sxybz'] == bz)]
                    sxy_xse_info[mc + 'hj_sn'] = jehj['jyje'].max()
                    sxy_xse_info[mc + 'ze' + '_pre'] = jehj['jyje'].max()
        pprint(sxy_xse_info)
        return sxy_xse_info

    def get_sxy_xse_bl(self):
        sxy_xse_bl = DataFrame(index=[0])
        qbxse_last = self.qbxse_last
        qbxse_pre = self.qbxse_pre
        sxy_xse_info = self.get_sxy_xse_info()
        sxymc = ['sy_', 'xy_']
        qbxse = [qbxse_last, qbxse_pre]
        for i in sxymc:
            for j in range(1, 11):
                sxy_xse_bl.loc[:, i + 'bl_qbxssr_' + str(j)] = np.divide(
                    sxy_xse_info[i + str(j)], qbxse_last)
                sxy_xse_bl.loc[:, i + 'bl_qbxssr_' + str(j) +
                               '_pre'] = np.divide(
                                   sxy_xse_info[i + str(j) + '_pre'],
                                   qbxse_pre)
                if j >= 2:
                    sxy_xse_bl.loc[:, i + 'hj_bl_qbxssr_' +
                                   str(j)] = np.divide(
                                       sxy_xse_info[i + 'hj_' + str(j)],
                                       qbxse_last)
                    sxy_xse_bl.loc[:, i + 'hj_bl_qbxssr_' + str(j) +
                                   '_pre'] = np.divide(
                                       sxy_xse_info[i + 'hj_' + str(j) +
                                                    '_pre'], qbxse_pre)
            for m in ['bn', 'sn']:
                if m == 'bn':
                    sxy_xse_bl.loc[:, i + 'bias'] = np.divide(
                        sxy_xse_info[i + 'hj_' + m] - qbxse_last, qbxse_last)
                else:
                    sxy_xse_bl.loc[:, i + 'bias' + '_pre'] = np.divide(
                        sxy_xse_info[i + 'hj_' + m] - qbxse_pre, qbxse_pre)
        pprint(sxy_xse_bl)
        return sxy_xse_bl

    def get_sxy_ch_count(self):
        sxy_ch_count = DataFrame(index=[0])
        sxy_xse_info = self.get_sxy_xse_info()
        data = self.sxy
        sxybz = [1, 0]
        chmc = ['gfnsrsbh', 'xfnsrsbh']
        for bz, nsr in zip(sxybz, chmc):
            for i in range(1, 11):
                nsr_last = set(data[(data['sssq'] == self.now_year)
                                    & (data['sxybz'] == bz) &
                                    (data['pm'] <= i)][nsr].unique())
                nsr_pre = set(data[(data['sssq'] == self.last_year)
                                   & (data['sxybz'] == bz) &
                                   (data['pm'] <= i)][nsr].unique())
                #重合个数
                ch_list = tuple(nsr_last.intersection(nsr_pre))
                df = data[data[nsr].isin(ch_list)]
                ch_bl = df.loc[:, ['gfnsrsbh', 'jyjebl', 'jyje']]
                ch_bl.drop_duplicates(inplace=True)
                pprint(ch_bl)
                if bz == 1:
                    sxy_ch_count.loc[:, 'xy_ch_count_' + str(i)] = len(ch_list)
                    sxy_ch_count['xy_ch_bl_syze_' + str(i)] = np.divide(
                        ch_bl['jyjebl'].sum(), sxy_xse_info['xy_ze'])
                    sxy_ch_count['xy_ch_bl_qbxssr_' + str(i)] = np.divide(
                        ch_bl['jyjebl'].sum(), self.qbxse_last)
                else:
                    sxy_ch_count.loc[:, 'sy_ch_count_' + str(i)] = len(ch_list)
                    sxy_ch_count['sy_ch_bl_syze_' + str(i)] = np.divide(
                        ch_bl['jyjebl'].sum(), sxy_xse_info['sy_ze'])
                    sxy_ch_count['sy_ch_bl_qbxssr_' + str(i)] = np.divide(
                        ch_bl['jyjebl'].sum(), self.qbxse_last)
        pprint(sxy_ch_count)
        return sxy_ch_count

    def get_concat_df(self):
        try:
            self.get_sxy_data()
        except:
            sxy_info=DataFrame(index=[0])
            sxy_info['nsrsbh']=self.nsrsbh
            sxy_info['remark']='申报缺失'
            return sxy_info
        sxy_xse_info = self.get_sxy_xse_info()
        sxy_xse_bl = self.get_sxy_xse_bl()
        sxy_ch_count = self.get_sxy_ch_count()
        col = [sxy_xse_info, sxy_xse_bl, sxy_ch_count]
        sxy_info = pd.concat(col, axis=1)
        sxy_info['nsrsbh']=self.nsrsbh
        xsy_info['remark']=None
        pprint(sxy_info)
        return sxy_info


if __name__ == '__main__':
    h = handle_sxyxx('91350211594998858T', '2017-03-01')
    h.get_sxy_data()
    #h.get_sxy_xse_info()
    #h.get_sxy_xse_bl()
    #h.get_sxy_ch_count()
    #h.get_concat_df()