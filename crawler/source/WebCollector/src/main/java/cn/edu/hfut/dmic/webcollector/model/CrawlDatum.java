/*
 * Copyright (C) 2015 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.util.CrawlDatumFormater;
import cn.edu.hfut.dmic.webcollector.util.GsonUtils;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * 爬取任务的数据结构
 *
 * @author hu
 */
public class CrawlDatum implements Serializable, MetaGetter, MetaSetter<CrawlDatum> {

    public final static int STATUS_DB_UNEXECUTED = 0;
    public final static int STATUS_DB_FAILED = 1;
    public final static int STATUS_DB_SUCCESS = 5;

    private String url = null;
    private long executeTime = System.currentTimeMillis();

    //private int httpCode = -1;
    private int status = STATUS_DB_UNEXECUTED;
    private int executeCount = 0;
    /**
     * 在WebCollector 2.5之后，不再根据URL去重，而是根据key去重
     * 可以通过getKey()方法获得CrawlDatum的key,如果key为null,getKey()方法会返回URL
     * 因此如果不设置key，爬虫会将URL当做key作为去重标准
     */
    private String key = null;

    /**
     * 在WebCollector 2.5之后，可以为每个CrawlDatum添加附加信息metaData
     * 附加信息并不是为了持久化数据，而是为了能够更好地定制爬取任务
     * 在visit方法中，可以通过page.getMetaData()方法来访问CrawlDatum中的metaData
     */
    private JsonObject metaData = new JsonObject();

    public CrawlDatum() {
    }

    public CrawlDatum(String url) {
        this.url = url;
    }
    
     public CrawlDatum(String url,String type) {
        this.url = url;
        type(type);
    }
     
    public boolean matchType(String type){
        if(type==null){
            return type()==null;
        }else{
            return type.equals(type());
        }
    }
    
        /**
     * 判断当前Page的URL是否和输入正则匹配
     *
     * @param urlRegex
     * @return
     */
    public boolean matchUrl(String urlRegex) {
        return Pattern.matches(urlRegex, url());
    }


    public CrawlDatum(String url, String[] metas) throws Exception {
        this(url);
        if (metas.length % 2 != 0) {
            throw new Exception("length of metas must be even");
        } else {
            for (int i = 0; i < metas.length; i += 2) {
                meta(metas[i * 2], metas[i * 2 + 1]);
            }
        }
    }


    public int incrExecuteCount(int count) {
        executeCount+=count;
        return executeCount;
    }
    
    public static final String META_KEY_TYPE="s_t";
    
    public String type(){
        return meta(META_KEY_TYPE);
    }
    
    public CrawlDatum type(String type){
        return meta(META_KEY_TYPE, type);
    }


    public String url() {
        return url;
    }
    
    public CrawlDatum url(String url) {
        this.url = url;
        return this;
    }

    /**
     * @deprecated 已废弃，使用url()代替
     */
    @Deprecated
    public String getUrl() {
        return url();
    }

    /**
     * @deprecated 使用url(String url)代替
     */
   @Deprecated
    public CrawlDatum setUrl(String url) {
        return url(url);
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long fetchTime) {
        this.executeTime = fetchTime;
    }

    public int getExecuteCount() {
        return executeCount;
    }

    public void setExecuteCount(int executeCount) {
        this.executeCount = executeCount;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }



//
//    public void meta(Document metaData) {
//        this.metaData = metaData;
//    }
//
//
//
//    public CrawlDatum meta(String key,String value){
//        this.metaData.put(key, value);
//        return this;
//    }

    @Override
    public JsonObject meta() {
        return metaData;
    }
    @Override
    public String meta(String key){
        JsonElement value = metaData.get(key);
        return (value==null || (value instanceof JsonNull))?null:value.getAsString();
    }

    @Override
    public int metaAsInt(String key){
        return metaData.get(key).getAsInt();
    }

    @Override
    public boolean metaAsBoolean(String key) {
        return metaData.get(key).getAsBoolean();
    }

    @Override
    public double metaAsDouble(String key) {
        return metaData.get(key).getAsDouble();
    }

    @Override
    public long metaAsLong(String key) {
        return metaData.get(key).getAsLong();
    }


//    @Deprecated
//    public CrawlDatum putMetaData(String key,String value){
//        return meta(key,value);
//    }
//
//    @Deprecated
//    public String getMetaData(String key){
//        return meta(key);
//    }

    public String briefInfo(){
        return String.format("CrawlDatum: %s (URL: %s)",key(),url());
    }
    
     public String key() {
        if (key == null) {
            return url;
        } else {
            return key;
        }
    }
     
     public CrawlDatum key(String key) {
        this.key = key;
        return this;
    }





    @Override
    public String toString() {
        return CrawlDatumFormater.datumToString(this);
    }


    @Override
    public CrawlDatum meta(JsonObject metaData) {
        this.metaData = metaData;
        return this;
    }

    @Override
    public CrawlDatum meta(String key, String value) {
        metaData.addProperty(key, value);
        return this;
    }

    @Override
    public CrawlDatum meta(String key, int value) {
        metaData.addProperty(key, value);
        return this;
    }

    @Override
    public CrawlDatum meta(String key, boolean value) {
        metaData.addProperty(key, value);
        return this;
    }

    @Override
    public CrawlDatum meta(String key, double value) {
        metaData.addProperty(key, value);
        return this;
    }

    @Override
    public CrawlDatum meta(String key, long value) {
        metaData.addProperty(key, value);
        return this;
    }


}
