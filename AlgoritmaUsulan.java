/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Comparison;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maghfirohlutfi
 */
public class AlgoritmaUsulan {

    ArrayList<Object> columnSelect = new ArrayList();
    ArrayList<Object> columnWhere = new ArrayList();
    ArrayList<Object> operatorWhere = new ArrayList();
    ArrayList<Object> syaratWhere = new ArrayList();
    ArrayList<Object> columnGroup = new ArrayList();
    ArrayList<Object> columnOrder = new ArrayList();

    public AlgoritmaUsulan() {
    }

    public void setCase1() {
//test1 start line
        columnSelect.add("HUB");
        columnSelect.add("JK");
//test1 end line
    }

    public void run() {
        try {
            Mongo m = new Mongo();
            DB db = m.getDB("Sakernas_variabel");
            DBCollection coll = db.getCollection("dokumen");

            //ColumnSelectList {}
            //metadataHasilSelect {}
            //survei {}
            //getColumnSelectList: ColumnSelectList--{column-i, ...,column-m}
            ArrayList<HashMap<Object, Object>> metadataHasilSelect = new ArrayList<>();
            ArrayList<Object> survei = new ArrayList<>();

            //for column-j E ColumnSelectList do
            for (Object column : columnSelect) {
                //get id_parent of column-j from metadata into id
                DBCollection coll1 = db.getCollection("metadata");
                BasicDBObject whereField1 = new BasicDBObject();
                whereField1.put("kode", column);
                BasicDBObject selectFields1 = new BasicDBObject();
                selectFields1.put("_id", 0);
                selectFields1.put("id_parent", 1);

                //get kode_field and id_survey where id_parent=id into metadataHasilSelect
                //add id_survey into survei
                DBCursor cursor1 = coll1.find(whereField1, selectFields1);
                if (cursor1.hasNext()) {
                    DBObject fi = cursor1.next();
                    BasicDBObject whereField2 = new BasicDBObject();
                    whereField2.put("id_parent", fi.get("id_parent"));
                    BasicDBObject selectField2 = new BasicDBObject();
                    selectField2.put("_id", 0);
                    selectField2.put("id_survey", 1);
                    selectField2.put("kode", 1);
                    DBCursor cursor2 = coll1.find(whereField2, selectField2);
                    HashMap<Object, Object> md = new HashMap<Object, Object>();
                    while (cursor2.hasNext()) {
                        DBObject hasil = cursor2.next();
                        Object id_survey = hasil.get("id_survey");
                        Object kode = hasil.get("kode");
                        md.put(id_survey, kode);
                        if (!survei.contains(id_survey)) {
                            survei.add(id_survey);
                        }
                    }
                    metadataHasilSelect.add(md);
                }
            }
            //columnWhereList {}
            //operatorWhereList {}
            //constraintWhereList {}
            //metadataHasilWhere {}
            //metadataHasilWhereOperator {}
            //metadataHasilWhereSyarat {}
            ArrayList<HashMap<Object, Object>> metadataHasilWhere = new ArrayList<>();
            ArrayList<HashMap<Object, Object>> metadataHasilWhereOperator = new ArrayList<>();
            ArrayList<HashMap<Object, Object>> metadataHasilWhereSyarat = new ArrayList<>();

            //getColumnWhereList: columnWhereList--{column-n, ...,column-s}
            //getOperatorWhereList: operatorWhereList--{operator-n, ...,operator-s}
            //getConstraintWhereList: constraintWhereList--{constraint-n, ...,constraint-s}
            //for column-k E ColumnWhereList do
            for (int k = 0; k < columnWhere.size(); k++) {
                //get id_parent of column-j from metadata into id
                String column = columnWhere.get(k).toString();
                DBCollection coll1 = db.getCollection("metadata");
                BasicDBObject whereField1 = new BasicDBObject();
                whereField1.put("kode", column);
                BasicDBObject selectFields1 = new BasicDBObject();
                selectFields1.put("_id", 0);
                selectFields1.put("id_parent", 1);

                //get kode_field and id_survey where id_parent=id into metadataHasilWhere
                DBCursor cursor1 = coll1.find(whereField1, selectFields1);
                if (cursor1.hasNext()) {
                    DBObject fi = cursor1.next();
                    BasicDBObject whereField2 = new BasicDBObject();
                    whereField2.put("id_parent", fi.get("id_parent"));
                    BasicDBObject selectField2 = new BasicDBObject();
                    selectField2.put("_id", 0);
                    selectField2.put("id_survey", 1);
                    selectField2.put("kode", 1);
                    DBCursor cursor2 = coll1.find(whereField2, selectField2);
                    HashMap<Object, Object> md = new HashMap<Object, Object>();
                    HashMap<Object, Object> mdoperator = new HashMap<Object, Object>();
                    HashMap<Object, Object> mdsyarat = new HashMap<Object, Object>();
                    while (cursor2.hasNext()) {
                        DBObject hasil = cursor2.next();
                        Object id_survey = hasil.get("id_survey");
                        Object kode = hasil.get("kode");
                        md.put(id_survey, kode);
                        mdoperator.put(id_survey, operatorWhere.get(k));
                        mdsyarat.put(id_survey, syaratWhere.get(k));
                    }
                    metadataHasilWhere.add(md);
                    //get operator-k and id_survey into metadataHasilWhereOperator
                    metadataHasilWhereOperator.add(mdoperator);
                    //get constraint-k and id_survey into metadataHasilWhereSyarat
                    metadataHasilWhereSyarat.add(mdsyarat);
                }
            }

            //columnOrderList {}
            //metadataHasilOrder {}
            ArrayList<HashMap<Object, Object>> metadataHasilOrder = new ArrayList<>();

            //getColumnOrderList: columnOrderList--{column-n, ...,column-s}
            //for column-k E getColumnOrderList do
            for (Object column : columnOrder) {
                //get id_parent of column-j from metadata into id
                DBCollection coll1 = db.getCollection("metadata");
                BasicDBObject whereField1 = new BasicDBObject();
                whereField1.put("kode", column);
                BasicDBObject selectFields1 = new BasicDBObject();
                selectFields1.put("_id", 0);
                selectFields1.put("id_parent", 1);
                DBCursor cursor1 = coll1.find(whereField1, selectFields1);
                if (cursor1.hasNext()) {
                    DBObject fi = cursor1.next();
                    BasicDBObject whereField2 = new BasicDBObject();
                    whereField2.put("id_parent", fi.get("id_parent"));
                    BasicDBObject selectField2 = new BasicDBObject();
                    selectField2.put("_id", 0);
                    selectField2.put("id_survey", 1);
                    selectField2.put("kode", 1);
                    //get kode_field and id_survey where id_parent=id into metadataHasilOrder
                    DBCursor cursor2 = coll1.find(whereField2, selectField2);
                    HashMap<Object, Object> md = new HashMap<Object, Object>();
                    while (cursor2.hasNext()) {
                        DBObject hasil = cursor2.next();
                        Object id_survey = hasil.get("id_survey");
                        Object kode = hasil.get("kode");
                        md.put(id_survey, kode);
                    }
                    metadataHasilOrder.add(md);
                }
            }
            //for id_survey E survei

            for (int i = 0; i < survei.size(); i++) {
                //construct query Q using attributes in metadataHasilSelect,metadataHasilWhere,metadataHasilOrder
                //execute Q
                //get result of Q
                //return and display Q
                BasicDBObject selectField3 = new BasicDBObject();
                BasicDBObject whereField3 = new BasicDBObject();
                BasicDBObject groupField3 = new BasicDBObject();
                BasicDBObject orderField3 = new BasicDBObject();
                Object idsurvey = survei.get(i);
                whereField3.put("id_survey", new BasicDBObject("$eq", idsurvey));

                Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
                DBObject groupFields;
                dbObjIdMap.put("id_survey", "$id_survey");
                for (int j = 0; j < metadataHasilSelect.size(); j++) {
                    HashMap<Object, Object> konversi = metadataHasilSelect.get(j);
                    Object kode = konversi.get(idsurvey);
                    if (!columnGroup.isEmpty()) {
                        dbObjIdMap.put(kode.toString(), "$" + kode);
                    } else {
                        selectField3.put(kode.toString(), 1);
                    }
                }
                for (int j = 0; j < metadataHasilWhere.size(); j++) {
                    HashMap<Object, Object> konversi = metadataHasilWhere.get(j);
                    Object kode = konversi.get(idsurvey);
                    Object operator = metadataHasilWhereOperator.get(j).get(idsurvey);
                    Object syarat = metadataHasilWhereSyarat.get(j).get(idsurvey);
                    BasicDBObject operatorsyarat = new BasicDBObject();
                    if (operator == "=") {
                        operatorsyarat = new BasicDBObject("$eq", syarat);
                    } else {

                    }
                    whereField3.put(kode.toString(), operatorsyarat);
                }
                for (int j = 0; j < metadataHasilOrder.size(); j++) {
                    HashMap<Object, Object> konversi = metadataHasilOrder.get(j);
                    Object kode = konversi.get(idsurvey);
                    orderField3.put(kode.toString(), 1);
                }
                if (columnGroup.size() > 0) {
                    groupFields = new BasicDBObject("_id", new BasicDBObject(dbObjIdMap));
                    if (columnGroup.get(0).equals("count")) {
                        groupFields.put("jumlah", new BasicDBObject("$sum", 1));
                    }

                    DBObject match = new BasicDBObject("$match", whereField3);
                    DBObject group = new BasicDBObject("$group", groupFields);
                    DBObject sort = new BasicDBObject("$sort", orderField3);

                    AggregationOutput output = coll.aggregate(match, group, sort);
                    Integer cek = 0;
                    for (Iterator iterator = output.results().iterator(); iterator.hasNext();) {
                        Object next = iterator.next();
//                        System.out.println(next);
                    }
                } else {
                    DBCursor cursor3 = coll.find(whereField3, selectField3);
                    cursor3.sort(orderField3);
//                        System.out.println("?");
                    while (cursor3.hasNext()) {
                        Object next = cursor3.next();
//                        System.out.println(next);
                    }
                }

            }
        } catch (UnknownHostException ex) {
            Logger.getLogger(DwMongoVariabel.class.getName()).log(Level.SEVERE, null, ex);
        }
//        System.gc();
    }

}
