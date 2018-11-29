package com.soto.dao;


import com.soto.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
