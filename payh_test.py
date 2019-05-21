from pprint import pprint
from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from payh_sbxx import handle
from payh_jcxx import handle_jcxx
from payh_zsxx import handle_zsxx
from payh_sxyxx import handle_sxyxx
from payh_cwzb import handle_cwzb
import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.abspath('Tools'))
import common
import datetime


class handle_test():
    def __init__(self, nsrsbh, sssqz_max=None):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh = nsrsbh
        self.sssqz_max = sssqz_max
        self.cw_data = None
        self.path = 'D:/业务文档/平安银行/'

    def check_data(self, df, df_sql):
        df_list = [df, df_sql]
        equal_list = []
        diff_list = []
        for ddf in df_list:
            selected_dtype = ddf.select_dtypes(include=['float'])
            ddf[selected_dtype.columns] = selected_dtype.applymap(
                lambda x: round(x, 2))
        concat_df = pd.concat(list(df_sql.align(df)), ignore_index=True)
        pprint('concat_df:')
        pprint(concat_df)
        for col in concat_df.columns:
            try:
                if concat_df.loc[0, col] == concat_df.loc[1, col]:
                    equal_list.append(col)
                else:
                    diff_list.append(col)
            except:
                    diff_list.append(col)
        diff_df = concat_df[diff_list]
        equal_df = concat_df[equal_list]
        return equal_df, diff_df

    def payh_sbxx_check(self):
        sql = '''select * from t_payh_sb where nsrsbh='{0}' '''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        sbxx_info = common.dataframe(result, col)
        #pprint(sbxx_info)
        sb_feature = handle(self.nsrsbh, self.sssqz_max).get_concat_df()
        #pprint(sb_feature)
        sb_equal, sb_diff = self.check_data(sb_feature, sbxx_info)
        pprint(sb_diff)
        sb_equal.to_csv(self.path + 'sb_equal.csv')
        sb_diff.to_csv(self.path + 'sb_diff.csv')

    def payh_jcxx_check(self):
        sql = '''select * from t_payh_jbxx_2 where nsrsbh='{0}' '''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        jcxx_info = common.dataframe(result, col)
        pprint('jcxx:')
        pprint(jcxx_info)
        jcxx_feature = handle_jcxx(self.nsrsbh, self.sssqz_max).get_concat_df()
        pprint('jcxx_feature:')
        pprint(jcxx_feature)
        jcxx_equal, jcxx_diff = self.check_data(jcxx_feature, jcxx_info)
        pprint(jcxx_diff)
        jcxx_equal.to_csv(self.path + 'jcxx_equal.csv')
        jcxx_diff.to_csv(self.path + 'jcxx_diff.csv')
        return jcxx_equal, jcxx_diff

    def payh_zsxx_check(self):
        sql = '''select * from t_payh_sbzs_xz where nsrsbh='{0}' '''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        zsxx_info = common.dataframe(result, col)
        pprint(zsxx_info)
        zsxx_feature = handle_zsxx(self.nsrsbh, self.sssqz_max).get_concat_df()
        pprint('zsxx_feature:')
        pprint(zsxx_feature)
        zsxx_equal, zsxx_diff = self.check_data(zsxx_feature, zsxx_info)
        pprint(zsxx_diff)
        zsxx_equal.to_csv(self.path + 'zsxx_equal.csv')
        zsxx_diff.to_csv(self.path + 'zsxx_diff.csv')
        return zsxx_equal, zsxx_diff

    def payh_sxyxx_check(self):
        sql = '''select * from t_payh_sxyxx_2 where nsrsbh='{0}' '''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        sxy_info = common.dataframe(result, col)
        pprint(sxy_info)
        sxy_feature = handle_sxyxx(self.nsrsbh,
                                   self.sssqz_max).get_concat_df()
        pprint(sxy_feature.head())
        sxy_equal, sxy_diff = self.check_data(sxy_feature, sxy_info)
        pprint(sxy_diff)
        sxy_equal.to_csv(self.path + 'sxy_equal.csv')
        sxy_diff.to_csv(self.path + 'sxy_diff.csv')
        return sxy_equal,sxy_diff

    def payh_cwzb_check(self):
        sql = '''select a.*,b.* from t_cwzb_list a,t_cwzb_list_1 b where a.nsrsbh=b.nsrsbh and a.nsrsbh='{0}' and rownum=1'''.format(
            self.nsrsbh)
        result, col = self.db_handle.query(sql)
        cwzb_info = common.dataframe(result, col)
        #pprint(cwzb_info)
        cwzb_feature=handle_cwzb(self.nsrsbh).get_concat_df()
        #pprint(cwzb_feature)
        cwzb_equal,cwzb_diff=self.check_data(cwzb_feature,cwzb_info)
        cwzb_equal.to_csv(self.path + 'cwzb_equal.csv')
        cwzb_diff.to_csv(self.path + 'cwzb_diff.csv')
        return cwzb_equal,cwzb_diff

    def main(self, nsrsbh, sssqz=None):
        #sb_equal, sb_diff = self.payh_sbxx_check(nsrsbh, sssqz)
        #data, self.sssqz_max = handle().get_sbxx_data(nsrsbh, sssqz)
        #jcxx_check = self.payh_jcxx_check(nsrsbh, self.sssqz_max)
        #zsxx_check=self.payh_zsxx_check(nsrsbh,self.sssqz_max)
        #sxyxx_check=self.payh_sxyxx_check(nsrsbh)
        #cwzb_check = self.payh_cwzb_check(nsrsbh)
        pass


if __name__ == '__main__':
    h = handle_test('91350211594998858T', '2017-03-31')
    #h.payh_sbxx_check()
    #pprint(sbxx_check)
    #h.payh_jcxx_check()
    #h.payh_zsxx_check()
    #h.payh_sxyxx_check()
    h.payh_cwzb_check()
    #h.main('91320114783833278X')
    #h.main('91321202557099212B')