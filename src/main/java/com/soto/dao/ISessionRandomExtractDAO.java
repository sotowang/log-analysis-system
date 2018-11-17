package com.soto.dao;

import com.soto.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);

}
