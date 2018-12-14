package com.ly.elasticsearchdemo.repository;

import com.ly.elasticsearchdemo.entity.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemRepository extends ElasticsearchRepository<Item, Long> {
    /**
     * 根据价格区间查询
     */
    List<Item> findByPriceBetween(Double price11,Double price2);
}
