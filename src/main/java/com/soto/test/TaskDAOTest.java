package com.soto.test;

import com.soto.dao.ITaskDAO;
import com.soto.dao.factory.DAOFactory;
import com.soto.domain.Task;

/**
 * 任务管理DAO测试类
 */
public class TaskDAOTest {
    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }
}
