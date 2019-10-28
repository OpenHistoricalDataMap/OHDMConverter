package inter2ohdm;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import java.sql.Connection;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import util.DB;
import util.Parameter;
import util.SQLStatementQueue;

class OHDMWayTest {
	static SQLStatementQueue sql;
	static Date importDatum;
	static Date updateDatum;
	static String parameterPfad = "";

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		Parameter parameter = new Parameter(parameterPfad);
		Connection connection = DB.createConnection(parameter);
		sql = new SQLStatementQueue(connection);
		Calendar myCal = Calendar.getInstance();
		myCal.set(Calendar.YEAR, 2000);
		myCal.set(Calendar.MONTH, 3);
		myCal.set(Calendar.DAY_OF_MONTH, 21);
		importDatum=myCal.getTime();
		myCal.set(Calendar.YEAR, 2001);
		updateDatum=myCal.getTime();
		System.out.print(updateDatum);
	}

	@Test
	void way_unveraendert() throws SQLException {
		sql.append("select * from geoobject where name='10-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if (res.next())
				fail();
			assertEquals(res.getDate("valid_since"),importDatum);
			assertEquals(res.getDate("valid_until"),updateDatum);
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			i=res.getInt("id_target");
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.059019404 0.02560207458,"
					+ "-0.05389900688 0.0286563462,-0.04626332696 0.0252427485)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_PunktVerschoben() throws SQLException {
		sql.append("select * from geoobject where name='11-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		int j=0;
		if (res.next()) {
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			if(res.getDate("valid_until")!=updateDatum) {
				assertEquals(res.getDate("valid_since"),importDatum);
				assertEquals(res.getDate("valid_until"),importDatum);
				i=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),updateDatum);
					assertEquals(res.getDate("valid_until"),updateDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					j=res.getInt("id_target");
				} else {fail();}
			} else {
				assertEquals(res.getDate("valid_since"),updateDatum);
				assertEquals(res.getDate("valid_until"),updateDatum);
				j=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),importDatum);
					assertEquals(res.getDate("valid_until"),importDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					i=res.getInt("id_target");
				} else {fail();}
			}
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.03799882635 0.02650038977,"
					+ "-0.03296826076 0.02425460178,-0.02838685281 0.02650038977)");
		}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(j);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.03799882635 0.02650038977,"  
					+ "-0.03296826076 0.02425460178,-0.02838685000 0.02650038000)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_PunktKommtDazu() throws SQLException {
		sql.append("select * from geoobject where name='12-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		int j=0;
		if (res.next()) {
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			if(res.getDate("valid_until")!=updateDatum) {
				assertEquals(res.getDate("valid_since"),importDatum);
				assertEquals(res.getDate("valid_until"),importDatum);
				i=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),updateDatum);
					assertEquals(res.getDate("valid_until"),updateDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					j=res.getInt("id_target");
				} else {fail();}
			} else {
				assertEquals(res.getDate("valid_since"),updateDatum);
				assertEquals(res.getDate("valid_until"),updateDatum);
				j=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),importDatum);
					assertEquals(res.getDate("valid_until"),importDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					i=res.getInt("id_target");
				} else {fail();}
			}
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.02057150984 0.02676988432,"
					+ "-0.01329515604 0.02461392786,-0.00619846529 0.02676988432)");
		}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(j);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.02057150984 0.02676988432,"  
					+ "-0.01329515604 0.02461392786,-0.00619846529 0.02676988432,-0.01619846529 0.01676988432)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_PunktWeniger() throws SQLException {
		sql.append("select * from geoobject where name='13-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		int j=0;
		if (res.next()) {
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			if(res.getDate("valid_until")!=updateDatum) {
				assertEquals(res.getDate("valid_since"),importDatum);
				assertEquals(res.getDate("valid_until"),importDatum);
				i=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),updateDatum);
					assertEquals(res.getDate("valid_until"),updateDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					j=res.getInt("id_target");
				} else {fail();}
			} else {
				assertEquals(res.getDate("valid_since"),updateDatum);
				assertEquals(res.getDate("valid_until"),updateDatum);
				j=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),importDatum);
					assertEquals(res.getDate("valid_until"),importDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					i=res.getInt("id_target");
				} else {fail();}
			}
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.00026958442 0.02721904192,"
					+ "0.00601862257 0.02470375938,0.00903597997 0.02626831491)");
		}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(j);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.00026958442 0.02721904192,"  
					+ "0.00601862257 0.02470375938)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_PunktKommtInMitteDazu() throws SQLException {
		sql.append("select * from geoobject where name='14-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		int j=0;
		if (res.next()) {
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			if(res.getDate("valid_until")!=updateDatum) {
				assertEquals(res.getDate("valid_since"),importDatum);
				assertEquals(res.getDate("valid_until"),importDatum);
				i=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),updateDatum);
					assertEquals(res.getDate("valid_until"),updateDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					j=res.getInt("id_target");
				} else {fail();}
			} else {
				assertEquals(res.getDate("valid_since"),updateDatum);
				assertEquals(res.getDate("valid_until"),updateDatum);
				j=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),importDatum);
					assertEquals(res.getDate("valid_until"),importDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					i=res.getInt("id_target");
				} else {fail();}
			}
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.01679840598 0.02641055825,"
					+ "0.02182897157 0.02560207458,0.02838667315 0.02676988432)");
		}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(j);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.01679840598 0.02641055825,"  
					+ "0.02182897157 0.02560207458,0.02582897157 0.02760207458,0.02838667315 0.02676988432)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_Neu() throws SQLException {
		sql.append("select * from geoobject where name='15-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if (res.next())
				fail();
			assertEquals(res.getDate("valid_since"),updateDatum);
			assertEquals(res.getDate("valid_until"),updateDatum);
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			i=res.getInt("id_target");
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.11679840598 0.12641055825,"
					+ "0.12182897157 0.12560207458,0.12838667315 0.12676988432)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_Geloescht() throws SQLException {
		sql.append("select * from geoobject where name='16-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if (res.next())
				fail();
			assertEquals(res.getDate("valid_since"),importDatum);
			assertEquals(res.getDate("valid_until"),importDatum);
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			i=res.getInt("id_target");
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.11679840598 0.12641055825,"
					+ "0.12182897157 0.12560207458,0.12838667315 0.12676988432)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_NeuesGeoobject() throws SQLException {
		sql.append("select * from geoobject where name='17-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if (res.next())
				fail();
			assertEquals(res.getDate("valid_since"),importDatum);
			assertEquals(res.getDate("valid_until"),importDatum);
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			i=res.getInt("id_target");
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.0188647108 0.01895454198,"
					+ "-0.01356465062 0.01616976473,-0.00691711752 0.0194036996)");
		}
		sql.resetStatement();
		sql.append("select * from geoobject where name='17-TC2'");
		res = sql.executeWithResult();
		i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if (res.next())
				fail();
			assertEquals(res.getDate("valid_since"),updateDatum);
			assertEquals(res.getDate("valid_until"),updateDatum);
			assertEquals(res.getInt("classification_id"),401);
			assertEquals(res.getInt("type_target"),2);
			i=res.getInt("id_target");
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(-0.0188647108 0.01895454198,"
					+ "-0.01356465062 0.01616976473,-0.00691711752 0.0194036996)");
		}
		sql.resetStatement();
	}
	
	@Test
	void way_ClassificationAendertSich() throws SQLException {
		sql.append("select * from geoobject where name='18-TC'");
		ResultSet res = sql.executeWithResult();
		int i = 0;
		if (res.next()) {
			i = res.getInt("Id");
			if(res.next())
				fail();
		} else {fail();}
		sql.resetStatement();
		sql.append("select * from geoobject_geometry where id_geoobject_source=");
		sql.append(i);
		res = sql.executeWithResult();
		int j=0;
		if (res.next()) {
			assertEquals(res.getInt("type_target"),2);
			if(res.getDate("valid_until")!=updateDatum) {
				assertEquals(res.getInt("classification_id"),401);
				assertEquals(res.getDate("valid_since"),importDatum);
				assertEquals(res.getDate("valid_until"),importDatum);
				i=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),updateDatum);
					assertEquals(res.getDate("valid_until"),updateDatum);
					assertEquals(res.getInt("classification_id"),424);
					assertEquals(res.getInt("type_target"),2);
					j=res.getInt("id_target");
				} else {fail();}
			} else {
				assertEquals(res.getInt("classification_id"),424);
				assertEquals(res.getDate("valid_since"),updateDatum);
				assertEquals(res.getDate("valid_until"),updateDatum);
				j=res.getInt("id_target");
				if(res.next()) {
					assertEquals(res.getDate("valid_since"),importDatum);
					assertEquals(res.getDate("valid_until"),importDatum);
					assertEquals(res.getInt("classification_id"),401);
					assertEquals(res.getInt("type_target"),2);
					i=res.getInt("id_target");
				} else {fail();}
			}
		} else {fail();}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(i);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.00287451908 0.01994268874,"
					+ "0.00871356842 0.01563077558,0.01419329166 0.01922403655)");
		}
		sql.resetStatement();
		sql.append("select ST_AsText(line) as linetext from lines where id=");
		sql.append(j);
		res = sql.executeWithResult();
		if (res.next()) {
			if(res.next())
				fail();
			assertEquals(res.getString("linetext"),"LINESTRING(0.00287451908 0.01994268874,"
					+ "0.00871356842 0.01563077558,0.01419329166 0.01922403655)");
		}
		sql.resetStatement();
	}

}