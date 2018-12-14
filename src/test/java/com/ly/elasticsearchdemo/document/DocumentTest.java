package com.ly.elasticsearchdemo.document;

import com.ly.elasticsearchdemo.entity.Item;
import com.ly.elasticsearchdemo.repository.ItemRepository;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DocumentTest {

    @Autowired
    private ItemRepository itemRepository;

    @Test
    public void testSave() {
        //单个添加
        Item item = new Item(1L, "小米8", "手机", "小米", 2299.00, "img13.360buyimg.com/12345.jpg");
        itemRepository.save(item);
        //批量添加
        List<Item> list = new ArrayList<>();
        list.add(new Item(2L, "荣耀V10", "手机", "华为", 2799.00, "img13.360buyimg.com/111.jpg"));
        list.add(new Item(3L, "坚果手机R1", "手机", "锤子", 3699.00, "img13.360buyimg.com/222.jpg"));
        list.add(new Item(4L, "华为meta10", "手机", "华为", 4499.00, "img13.360buyimg.com/333.jpg"));
        list.add(new Item(5L, "小米Mix2S", "手机", "小米", 4299.00, "img13.360buyimg.com/444.jpg"));
        itemRepository.saveAll(list);
    }

    @Test
    public void testFindAll() {
        Iterable<Item> items = itemRepository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        items.forEach(item -> System.out.println(item));
    }

    @Test
    public void queryByPriceBetween() {
        List<Item> items = itemRepository.findByPriceBetween(4000.00, 5000.00);
        items.forEach(item -> System.out.println(item));
    }

    @Test
    public void testQuery() {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        Iterable<Item> items = itemRepository.search(queryBuilder);
        items.forEach(System.out::println);
    }

    @Test
    public void testNativeQuery() {
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米"));
        // 执行搜索，获取结果
        Page<Item> items = itemRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println(items.getTotalElements());
        // 打印总页数
        System.out.println(items.getTotalPages());
        items.forEach(System.out::println);
    }

    @Test
    public void testNativePageQuery() {
        //设置分页
        int page = 0;
        int size = 3;
        //构建查询条件
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("category", "手机"))
                .withPageable(PageRequest.of(page, size)).build();
        //获取结果
        Page<Item> items = itemRepository.search(searchQuery);
        System.out.println(String.format("总页数：%s", items.getTotalPages()));
        System.out.println(String.format("总条数：%s", items.getTotalElements()));
        System.out.println(String.format("每页大小：%s", items.getSize()));
        System.out.println(String.format("当前页：%s", items.getNumber()));
        items.forEach(System.out::println);
    }

    @Test
    public void testSort() {
        // 构建查询条件
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("category", "手机"))
                .withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC)).build();
        // 执行搜索，获取结果
        Page<Item> items = itemRepository.search(searchQuery);
        System.out.println(String.format("总条数：%s", items.getTotalElements()));
        items.forEach(System.out::println);
    }

    @Test
    public void testAgg() {
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                //不查询任何结果
                .withSourceFilter(new FetchSourceFilter(new String[]{""}, null))
                // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
                .addAggregation(AggregationBuilders.terms("brands").field("brand"))
                .build();
        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) itemRepository.search(searchQuery);
        //3.解析
        //3.1取出名为brands的聚合
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        //3.2获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        //3.3遍历
        buckets.forEach(bucket ->
                System.out.println(String.format("桶的key：%s,文档数量：%s", bucket.getKeyAsString(), bucket.getDocCount()))
        );
    }

    @Test
    public void testSubAgg() {
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withSourceFilter(new FetchSourceFilter(new String[]{}, null))
                .addAggregation(
                        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
                        AggregationBuilders.terms("brands").field("brand")
                                // 在品牌聚合桶内进行嵌套聚合，求平均值
                                .subAggregation(AggregationBuilders.avg("priceAvg").field("price"))
                ).build();

        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) itemRepository.search(searchQuery);
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3 遍历
        buckets.forEach(bucket -> {
            // 3.4 获取桶中的key和文档数量
            System.out.println(String.format("桶的key：%s,文档数量：%s", bucket.getKeyAsString(), bucket.getDocCount()));
            // 3.5 获取子聚合结果
            InternalAvg avg = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println("平均售价：" + avg.getValue());
        });
    }
}
