package com.ylc.jpa;

import com.ylc.entity.Stat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Map;

public interface StatJPA  extends JpaRepository<Stat, Long> {
    @Query(value = "select lng,lat,count(1) as count from stat where time > unix_timestamp(date_sub(current_timestamp(),interval 1000 minute))*1000 group by lng,lat", nativeQuery=true)
    List<Map> findData();
}
