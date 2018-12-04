package com.soto;

import com.soto.dao.IAdUserClickCountDAO;
import com.soto.domain.AdUserClickCount;
import com.soto.jdbc.JDBCHelper;
import com.soto.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdUserClickCountDAOImplTest implements IAdUserClickCountDAO {

    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 首先对用户广告点击量进行分类，分成待插入的和待更新的
        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();

        String selectSQL = "SELECT count(*) FROM ad_user_click_count "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        Object[] selectParams = null;

        for (AdUserClickCount adUserClickCount : adUserClickCounts) {

            final AdUserClickCountQueryResult adUserClickCountQueryResult = new AdUserClickCountQueryResult();

            String date = adUserClickCount.getDate();
            long user_id = adUserClickCount.getUserid();
            long ad_id = adUserClickCount.getAdid();

            selectParams = new Object[]{date, user_id, ad_id};

            jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        adUserClickCountQueryResult.setCount(rs.getInt(1));
                    }
                }
            });

            int count = adUserClickCountQueryResult.getCount();
            if (count > 0) {
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
    public int findClickCountByMultiKey(String date, long userid, final long adid) {
        String sql = "SELECT click_count "
                + "FROM ad_user_click_count "
                + "WHERE date=? "
                + "AND user_id=? "
                + "AND ad_id=?";

        Object[] params = new Object[]{date, userid, adid};

        final AdUserClickCountQueryResult adUserClickCountQueryResult = new AdUserClickCountQueryResult();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    adUserClickCountQueryResult.setClickCount(rs.getInt(1));
                }
            }
        });
        int clickCount = adUserClickCountQueryResult.getClickCount();

        return clickCount;
    }


}
