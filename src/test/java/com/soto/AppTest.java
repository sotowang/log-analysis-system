package com.soto;

import static org.junit.Assert.assertTrue;

import com.soto.dao.IAdUserClickCountDAO;
import com.soto.dao.factory.DAOFactory;
import com.soto.domain.AdUserClickCount;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    public static void main(String[] args) {
        AdUserClickCountDAOImplTest adUserClickCountDAO = new AdUserClickCountDAOImplTest();
        int count = adUserClickCountDAO.findClickCountByMultiKey("2018-12-04", 2, 2);


        List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
        for (int i = 1; i < 10; i++) {
            AdUserClickCount adUserClickCount = new AdUserClickCount();
            adUserClickCount.setDate("2018-12-04");
            adUserClickCount.setUserid(2);
//            adUserClickCount.setAdid(3);
            adUserClickCount.setClickCount(1);

            adUserClickCounts.add(adUserClickCount);
        }
        adUserClickCountDAO.updateBatch(adUserClickCounts);
    }
}
