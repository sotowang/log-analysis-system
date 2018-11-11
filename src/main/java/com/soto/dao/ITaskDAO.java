package com.soto.dao;

import com.soto.domain.Task;

public interface ITaskDAO {
    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findById(long taskid);
}
