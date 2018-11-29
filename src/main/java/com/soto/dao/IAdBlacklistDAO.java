package com.soto.dao;

import com.soto.domain.AdBlacklist;

import java.util.List;

/**
 * 广告黑名单DAO接口
 */
public interface IAdBlacklistDAO {

    /**
     * 批量插入广告黑名单用户
     * @param adBlacklists
     */
    void insertBatch(List<AdBlacklist> adBlacklists);

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    List<AdBlacklist> findAll();
}
