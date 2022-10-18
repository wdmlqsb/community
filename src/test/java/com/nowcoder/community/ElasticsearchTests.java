package com.nowcoder.community;

import com.nowcoder.community.dao.DiscussPostMapper;
import com.nowcoder.community.dao.elasticsearch.DiscussPostRepository;
import com.nowcoder.community.entity.DiscussPost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = CommunityApplication.class)
public class ElasticsearchTests {

    // 数据来源是MySQL，数据库中取帖子，注入DiscussPostMapper
    @Autowired
    private DiscussPostMapper discussMapper;

    // 存取数据到ES 注入DiscussPostRepository
    @Autowired
    private DiscussPostRepository discussRepository;

    // 有些数据处理不了，注入ElasticsearchTemplate
    @Autowired
    private ElasticsearchTemplate elasticTemplate;

    // 往ES里添加数据
    @Test
    public void testInsert() {
        discussRepository.save(discussMapper.selectDiscussPostById(241));
        discussRepository.save(discussMapper.selectDiscussPostById(242));
        discussRepository.save(discussMapper.selectDiscussPostById(243));
    }

    // 往ES里添加多条数据
    @Test
    public void testInsertList() {
        discussRepository.saveAll(discussMapper.selectDiscussPosts(101, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(102, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(103, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(111, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(112, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(131, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(132, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(133, 0, 100, 0));
        discussRepository.saveAll(discussMapper.selectDiscussPosts(134, 0, 100, 0));
    }

    // 修改内容
    @Test
    public void testUpdate() {
        DiscussPost post = discussMapper.selectDiscussPostById(231);
        post.setContent("我是新人,使劲灌水.");
        discussRepository.save(post);
    }

    // 删除 一条/所有
    @Test
    public void testDelete() {
        // discussRepository.deleteById(231);
        discussRepository.deleteAll();
    }

    // 搜索测试，利用接口搜索
    // 构造查询条件，利用接口SearchQuery，实现类是NativeSearchQuery，NativeSearchQueryBuilder工具类可以构造一个实现类NativeSearchQuery
    // 构造排序条件，type置顶，status加精score分高，创建时间
    // 构造分页条件，第几页，显示多少条数据
    // 指定高亮显示，指定多个字段（让里面包含的关键词高亮），前后加标签
    //调用NativeSearchQueryBuilder方法的bulid，返回searchQuery接口的实现类
    @Test
    public void testSearchByRepository() {
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.multiMatchQuery("互联网寒冬", "title", "content"))
                .withSort(SortBuilders.fieldSort("type").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("score").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("createTime").order(SortOrder.DESC))
                .withPageable(PageRequest.of(0, 10))
                .withHighlightFields(
                        new HighlightBuilder.Field("title").preTags("<em>").postTags("</em>"),
                        new HighlightBuilder.Field("content").preTags("<em>").postTags("</em>")
                ).build();

        // elasticTemplate.queryForPage(searchQuery, class, SearchResultMapper)
        // 底层获取得到了高亮显示的值, 但是没有返回.

        // 分页查询，返回分页数据 Page看成集合，里面封装了多个实体类
        Page<DiscussPost> page = discussRepository.search(searchQuery);
        System.out.println(page.getTotalElements());
        System.out.println(page.getTotalPages());
        System.out.println(page.getNumber());
        System.out.println(page.getSize());
        // post是从ES服务器中搜出来的
        for (DiscussPost post : page) {
            System.out.println(post);
        }
    }

    //底层获取得到了高亮显示的值, 但是没有返回.直接用底层elasticTemplate方法
    @Test
    public void testSearchByTemplate() {
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.multiMatchQuery("互联网寒冬", "title", "content"))
                .withSort(SortBuilders.fieldSort("type").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("score").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("createTime").order(SortOrder.DESC))
                .withPageable(PageRequest.of(0, 10))
                .withHighlightFields(
                        new HighlightBuilder.Field("title").preTags("<em>").postTags("</em>"),
                        new HighlightBuilder.Field("content").preTags("<em>").postTags("</em>")
                ).build();

        // 用elasticTemplate做查询
        Page<DiscussPost> page = elasticTemplate.queryForPage(searchQuery, DiscussPost.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> aClass, Pageable pageable) {
                SearchHits hits = response.getHits();
                // 没查到数据
                if (hits.getTotalHits() <= 0) {
                    return null;
                }

                // 数据封装到集合里
                List<DiscussPost> list = new ArrayList<>();
                // 遍历命令数据，处理，放集合里
                for (SearchHit hit : hits) {
                    // 每得到一个数据，包装返回到实体类DiscussPost里面去
                    DiscussPost post = new DiscussPost();

                    // 返回map类型的数据，转成字符串
                    String id = hit.getSourceAsMap().get("id").toString();
                    // 存的时候，再转换一下
                    post.setId(Integer.valueOf(id));

                    String userId = hit.getSourceAsMap().get("userId").toString();
                    post.setUserId(Integer.valueOf(userId));

                    // 先获取原始的
                    // 再查有没有，有就覆盖成高亮显示的内容
                    String title = hit.getSourceAsMap().get("title").toString();
                    post.setTitle(title);

                    String content = hit.getSourceAsMap().get("content").toString();
                    post.setContent(content);

                    String status = hit.getSourceAsMap().get("status").toString();
                    post.setStatus(Integer.valueOf(status));

                    //ES类型 时间是long
                    String createTime = hit.getSourceAsMap().get("createTime").toString();
                    post.setCreateTime(new Date(Long.valueOf(createTime)));

                    String commentCount = hit.getSourceAsMap().get("commentCount").toString();
                    post.setCommentCount(Integer.valueOf(commentCount));

                    // 处理高亮显示的结果
                    // 匹配到的那一段返回
                    HighlightField titleField = hit.getHighlightFields().get("title");
                    if (titleField != null) {
                        post.setTitle(titleField.getFragments()[0].toString());
                    }

                    HighlightField contentField = hit.getHighlightFields().get("content");
                    if (contentField != null) {
                        post.setContent(contentField.getFragments()[0].toString());
                    }

                    // 放到集合里
                    list.add(post);
                }

                return new AggregatedPageImpl(list, pageable,
                        hits.getTotalHits(), response.getAggregations(), response.getScrollId(), hits.getMaxScore());
            }
        });

        System.out.println(page.getTotalElements());
        System.out.println(page.getTotalPages());
        System.out.println(page.getNumber());
        System.out.println(page.getSize());
        for (DiscussPost post : page) {
            System.out.println(post);
        }
    }

}
