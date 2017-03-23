/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MultiversionDataWarehouse;

import Utility.KoneksiPostgreSQL;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import static java.lang.System.out;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maghfirohlutfi
 */
public class AlgoritmaPlain {

    String[] sMVQ = new String[11];

    public AlgoritmaPlain() {
//            test1
        sMVQ[0] = "select B4_K3,B4_K4 from dokumen";

        //test2
        sMVQ[1] = "select B1P02,	NAMA_KAB, B4_K3,B4_K4 \n"
                + "from dokumen \n";
//            //test3
        sMVQ[2] = "select B1P02,	NAMA_KAB, B4_K3,B4_K5 \n"
                + "from dokumen where B4_K4=1\n";
        //test4
        sMVQ[3] = "select B1P02,	NAMA_KAB, B4_K3,B4_K5 \n"
                + "from dokumen where B4_K4=1\n"
                + "order by B1P02";
        //test5
        sMVQ[4] = "select B1P02,	NAMA_KAB, B4_K3,B5P1A \n"
                + "from dokumen \n";
        //test6
        sMVQ[5] = "select B1P02,	NAMA_KAB, B4_K3,B5P1A \n"
                + "from dokumen where B4_K4=1\n";
        //test7
        sMVQ[6] = "select B1P02,	NAMA_KAB, B4_K3,B5P1A \n"
                + "from dokumen where B4_K4=1\n"
                + "order by B1P02";
        //test8
        sMVQ[7] = "select B1P02,	NAMA_KAB, count(*) \n"
                + "from dokumen \n"
                + "group by B1P02,	NAMA_KAB";
        //test9
        sMVQ[8] = "select B1P02,	B4_K4, count(*) \n"
                + "from dokumen \n"
                + "group by B1P02,	B4_K4";
        //test10
        sMVQ[9] = "select B1P02,	B4_K4, count(*) \n"
                + "from dokumen where B5P1A=10"
                + "group by B1P02,	B4_K4";
        //test11
        sMVQ[10] = "select 	B4_K4,B5P1A, count(*) \n"
                + "from dokumen  "
                + "group by B4_K4,B5P1A";
    }

    public void run(int noTest) {
        try {
            KoneksiPostgreSQL kon = new KoneksiPostgreSQL("metaschema_dwdm");
            //1. input
            String MVQ = sMVQ[noTest];
            //2. Q={}{the set of partial query}
            ArrayList<TStatementList> Q = new ArrayList<>();
            //3. V={}{the set of versions being addressed in MVQ}
            ArrayList<Integer> V = new ArrayList<>();
            HashMap<Integer, String> Vname = new HashMap<>();
            //4. T={}{the set of tables being addressed in MVQ}
            ArrayList<String> T = new ArrayList<>();
            //5. A={}{the set of attribute being addressed in MVQ}
            ArrayList<String> A = new ArrayList<>();
            //6. get dw version
            String vsql = "select ver_id,ver_name from skema_public.versions where ver_bvt between '01-01-2010' and '31-12-2015'";
            try {
                Statement stmt = kon.getKoneksi().createStatement();
                ResultSet rs = stmt.executeQuery(vsql);
                while (rs.next()) {
                    Integer ver_id = rs.getInt(1);
                    V.add(ver_id);
                    Vname.put(ver_id, rs.getString(2));
                }
                stmt.close();
            } catch (SQLException ex) {
                Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
            }
            Parser parser = new Parser(MVQ);
            //7. get table from MVQ??
            T = parser.getFoundTables();
            HashMap<Integer, String> Tmap = new HashMap<>();
            for (int t = 0; t < T.size(); t++) {
                Tmap.put(t, T.get(t));
            }
            //8. get attribute from MVQ??
            A = parser.getFoundColumns();
            HashMap<Integer, String> Amap = new HashMap<>();
            for (int a = 0; a < T.size(); a++) {
                Amap.put(a, T.get(a));
            }
            //9.
            for (Integer Vj : V) {
                //10
                HashMap<Integer, String> Ttemp = new HashMap<>();
                //11
//                ArrayList<String> Atemp = new ArrayList<>();
                HashMap<Integer, String> Atemp = new HashMap<>();
                //12
                Boolean construct = true;
                //13
                //check if all table in T exist in Vj {consult the metadata}
                Boolean alltable = true;
                //alltable=true jika semua, false jika tidak.
                for (String tabel : T) {
                    String cekTabel = "select fv_id from skema_public.fact_versions where fv_name='" + tabel + "' and ver_id=" + Vj + ";";
                    try {
                        Statement stmt = kon.getKoneksi().createStatement();
                        ResultSet rs = stmt.executeQuery(cekTabel);
                        if (rs.next()) {
                            alltable = false;
                            break;
                        }
                        stmt.close();
                    } catch (SQLException ex) {
                        Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
//                14
                if (!alltable) {
                    //15
                    //check if table name change in Vj {consult the metadata}
                    for (int tab = 0; tab < T.size(); tab++) {
                        String tabel = T.get(tab);
                        String cekTabel = "select fv_id from skema_public.fact_versions where fv_name='" + tabel + "';";
                        //16
                        try {
                            Statement stmt = kon.getKoneksi().createStatement();
                            ResultSet rs = stmt.executeQuery(cekTabel);
                            if (!rs.next()) {
                                alltable = false;
                                //17
                                construct = false;//skip constructing partial query Qj
                                break;
                                //18
                            } else {
                                Integer id = rs.getInt(1);
                                String cekId = "select fv_name from skema_public.fact_versions where ver_id=" + Vj + " and fv_id in "
                                        + "((select fv_id_old from skema_public.fact_ver_map where fv_id_new=" + id + ") "
                                        + "union "
                                        + "(select fv_id_new from skema_public.fact_ver_map where fv_id_old=" + id + "))";
                                Statement stmt1 = kon.getKoneksi().createStatement();
                                ResultSet rs1 = stmt1.executeQuery(cekId);
                                if (rs1.next()) {
                                    //19
                                    //get table names into Ttemp;
                                    Ttemp.put(tab, rs1.getString("fv_name"));
                                }
                                stmt1.close();
                                while (rs.next()) {
                                    Integer id1 = rs.getInt(1);
                                    String cekId1 = "select fv_name from skema_public.fact_versions where ver_id=" + Vj + " and fv_id in "
                                            + "((select fv_id_old from skema_public.fact_ver_map where fv_id_new=" + id1 + ") "
                                            + "union "
                                            + "(select fv_id_new from skema_public.fact_ver_map where fv_id_old=" + id1 + "))";
                                    Statement stmt11 = kon.getKoneksi().createStatement();
                                    ResultSet rs11 = stmt11.executeQuery(cekId1);
                                    if (rs11.next()) {
                                        //19
                                        //get table names into Ttemp;
                                        Ttemp.put(tab, rs11.getString("fv_name"));
                                    }
                                    stmt11.close();
                                }
                                //20
                            }
                            stmt.close();
                        } catch (SQLException ex) {
                            Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    //21
                } else {
                    //22
                    //get table names into Ttemp;
                    for (int tab = 0; tab < T.size(); tab++) {
                        String tabel = T.get(tab);
                        Ttemp.put(tab, tabel);
                    }
//23
                }
                //24
                //check if all attribute in A exist in Ttemp {consult the metadata}
                Boolean allatt = true;
                //all //alltable=true jika semua, false jika tidak.
                for (String att : A) {
                    String cekAtt = "select att_id from skema_public.attributes a,skema_public.fact_versions b where a.fv_id=b.fv_id and att_name='" + att + "' and ver_id=" + Vj + ";";
                    try {
                        Statement stmt = kon.getKoneksi().createStatement();
                        ResultSet rs = stmt.executeQuery(cekAtt);
                        if (!rs.next()) {
                            allatt = false;
                            break;
                        }
                        stmt.close();
                    } catch (SQLException ex) {
                        Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                //25
                if (!allatt) {
                    //26
                    //check if attribute name change in Vj {consult the metadata}
                    for (int tab = 0; tab < A.size(); tab++) {
                        String att = A.get(tab);
                        String cekAtt = "select att_id from skema_public.attributes where att_name='" + att + "';";
                        try {
                            Statement stmt = kon.getKoneksi().createStatement();
                            ResultSet rs = stmt.executeQuery(cekAtt);
                            if (!rs.next()) {
                                allatt = false;
                                construct = false;//skip constructing partial query Qj
                                break;
                            } else {
                                Integer id = rs.getInt(1);
                                while (true) {
                                    String idx = "(select att_id_old from skema_public.att_map where att_id_new=" + id + ") ";
                                    Statement stmtIdx = kon.getKoneksi().createStatement();
                                    ResultSet rsIdx = stmtIdx.executeQuery(idx);
                                    if (rsIdx.next()) {
                                        //get attribute names into Atemp;
                                        String cekId = "select att_name from skema_public.attributes a, skema_public.fact_versions b where a.fv_id=b.fv_id and ver_id=" + Vj + " and att_id = "
                                                + rsIdx.getInt(1);
                                        Statement stmt1 = kon.getKoneksi().createStatement();
                                        ResultSet rs1 = stmt1.executeQuery(cekId);
                                        if (rs1.next()) {
                                            //get attribute names into Atemp;
                                            Atemp.put(tab, rs1.getString("att_name"));
                                            break;
                                        } else {
                                            id = rsIdx.getInt(1);
                                        }
                                        stmt1.close();
                                    } else {
                                        break;
                                    }
                                    stmtIdx.close();
                                }
                                while (true) {
                                    String idx = "(select att_id_new from skema_public.att_map where att_id_old=" + id + ") ";
                                    Statement stmtIdx = kon.getKoneksi().createStatement();
                                    ResultSet rsIdx = stmtIdx.executeQuery(idx);
                                    if (rsIdx.next()) {
                                        //get attribute names into Atemp;
                                        String cekId = "select att_name from skema_public.attributes a, skema_public.fact_versions b where a.fv_id=b.fv_id and ver_id=" + Vj + " and att_id = "
                                                + rsIdx.getInt(1);
                                        Statement stmt1 = kon.getKoneksi().createStatement();
                                        ResultSet rs1 = stmt1.executeQuery(cekId);
                                        if (rs1.next()) {
                                            //get attribute names into Atemp;
                                            Atemp.put(tab, rs1.getString("att_name"));
                                            break;
                                            //                                    System.out.println("berubah " + Atemp);
                                        } else {
                                            id = rsIdx.getInt(1);
                                        }
                                        stmt1.close();
                                    } else {
                                        break;
                                    }
                                    stmtIdx.close();
                                }
                                while (rs.next()) {
                                    id = rs.getInt(1);
                                    while (true) {
                                        String idx = "(select att_id_old from skema_public.att_map where att_id_new=" + id + ") ";
                                        Statement stmtIdx = kon.getKoneksi().createStatement();
                                        ResultSet rsIdx = stmtIdx.executeQuery(idx);
                                        if (rsIdx.next()) {
                                            //get attribute names into Atemp;
                                            String cekId = "select att_name from skema_public.attributes a, skema_public.fact_versions b where a.fv_id=b.fv_id and ver_id=" + Vj + " and att_id = "
                                                    + rsIdx.getInt(1);
                                            Statement stmt1 = kon.getKoneksi().createStatement();
                                            ResultSet rs1 = stmt1.executeQuery(cekId);
                                            if (rs1.next()) {
                                                //get attribute names into Atemp;
                                                Atemp.put(tab, rs1.getString("att_name"));
                                                break;
                                            } else {
                                                id = rsIdx.getInt(1);
                                            }
                                            stmt1.close();
                                        } else {
                                            break;
                                        }
                                        stmtIdx.close();
                                    }
                                    while (true) {
                                        String idx = "(select att_id_new from skema_public.att_map where att_id_old=" + id + ") ";
                                        Statement stmtIdx = kon.getKoneksi().createStatement();
                                        ResultSet rsIdx = stmtIdx.executeQuery(idx);
                                        if (rsIdx.next()) {
                                            //get attribute names into Atemp;
                                            String cekId = "select att_name from skema_public.attributes a, skema_public.fact_versions b where a.fv_id=b.fv_id and ver_id=" + Vj + " and att_id = "
                                                    + rsIdx.getInt(1);
                                            Statement stmt1 = kon.getKoneksi().createStatement();
                                            ResultSet rs1 = stmt1.executeQuery(cekId);
                                            if (rs1.next()) {
                                                //get attribute names into Atemp;
                                                Atemp.put(tab, rs1.getString("att_name"));
                                                break;
                                            } else {
                                                id = rsIdx.getInt(1);
                                            }
                                            stmt1.close();
                                        } else {
                                            break;
                                        }
                                        stmtIdx.close();
                                    }
                                }
                            }
                            stmt.close();
                        } catch (SQLException ex) {
                            Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    //27
                } else {
                    //22
                    //get table names into Ttemp;
                    for (int tab = 0; tab < A.size(); tab++) {
                        String att = A.get(tab);
                        Atemp.put(tab, att);
                    }
//23
                }
                //28
                if (construct) {
                    //29
                    //get attribute name into Atemp
                    //30
                    //construct partial query Qj using table name in Ttemp and attribute in Atemp.
                    TStatementList Qj = parser.getSqlstatements();

                    for (int q = 0; q < Qj.size(); q++) {
                        TSelectSqlStatement select = (TSelectSqlStatement) Qj.get(q);
                        TResultColumnList columns = select.getResultColumnList();
                        for (int col = 0; col < A.size(); col++) {
                            String kolAwal = columns.getResultColumn(col).toString();
                            String kolAkhir = Atemp.get(col);
                            columns.getResultColumn(col).setString(kolAkhir);
                        }
                        TTableList tabels = select.tables;
                        for (int col = 0; col < T.size(); col++) {
                            String tabAwal = tabels.getTable(col).toString();
                            String tabAkhir = Ttemp.get(col);
                            tabels.getTable(col).setString(Vname.get(Vj) + "." + tabAkhir);
                        }
                    }
                    //insert Qj into Q
                    Q.add(Qj);
                    //32
                }
                //33
            }
            //34
            for (TStatementList Qj : Q) {
                //35
                //execute Qj;
                for (int p = 0; p < Qj.size(); p++) {
                    Statement stmt = kon.getKoneksi().createStatement();
                    ResultSet rs = stmt.executeQuery(Qj.get(p).toString());
                    //36
                    //getreseult of Qj
                    while (rs.next()) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            if (i > 1) {
//                                out.print(",");
                            }
//                37
//            return and display Qj

                            int type = rsmd.getColumnType(i);
                            if (type == Types.VARCHAR || type == Types.CHAR) {
//                                out.print(rs.getString(i));
                            } else {
//                                out.print(rs.getLong(i));
                            }
                        }

//                        out.println();
                    }

//            38
                    stmt.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(AlgoritmaPlain.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.gc();
    }

}
