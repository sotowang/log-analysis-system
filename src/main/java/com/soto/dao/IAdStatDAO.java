package com.soto.dao;

import com.soto.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);

}
