package com.soto.dao.impl;

import com.soto.dao.IAdUserClickCountDAO;
import com.soto.domain.AdUserClickCount;
import com.soto.jdbc.JDBCHelper;
import com.soto.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户广告点击量DAO实现类
 *
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 首先对用户广告点击量进行分类，分成待插入的和待更新的
        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();


        String selectSQL = "SELECT count(*) FROM ad_user_click_count "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        Object[] selectParams = null;

        for(AdUserClickCount adUserClickCount : adUserClickCounts) {
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

            selectParams = new Object[]{adUserClickCount.getDate(),
                    adUserClickCount.getUserid(), adUserClickCount.getAdid()};

            jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {

                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()) {
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });

            int count = queryResult.getCount();

            if(count > 0) {
                updateAdUserClickCounts.add(adUserClickCount);
            } else {
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        // 执行批量插入
        String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();

        for(AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = new Object[]{adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid(),
                    adUserClickCount.getClickCount()};
            insertParamsList.add(insertParams);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);



        String updateSQL = "UPDATE ad_user_click_count SET click_count=click_count+? "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();

        for(AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
            Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()};
            updateParamsList.add(updateParams);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamsList);

    }

    @Override
    public int findClickCountByMultiKey(String date, long userid, long adid) {
        return 0;
    }
}
