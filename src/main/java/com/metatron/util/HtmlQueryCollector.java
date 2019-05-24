package com.metatron.util;


import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;



public class HtmlQueryCollector {

    public ArrayList<String> collectSqls(String url) {

        ArrayList<String> sqls = new ArrayList<String>();

        try{
            Document doc = Jsoup.connect(url).get();

            Elements contents;

            //contents = doc.select("pre[class*=sql]");

            contents = doc.select("pre[class~=^(wp-block-syntaxhighlighter-code)(.*)sql(.*)]");

            for(Element content : contents){
                System.out.println(content);
                System.out.println(content.text());
                for(String sql : content.text().split(";") ){
                    sqls.add(sql);
                }

            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return sqls;
    }




}
