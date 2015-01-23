package atomodo.fecscraper

import java.io.File
import java.util.Calendar
import java.util.regex.Matcher

import groovy.sql.Sql
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipFile

class DBModel {
	
	private File dataFolder
	def db
	Boolean fullUpdate = false
	Boolean theresBeenAnError = false
	String longMessage = ""
	
	public DBModel(File dataFolder) {
		this()
		this.dataFolder = dataFolder
	}

	public DBModel() {
		db = Sql.newInstance("jdbc:mysql://data.atomodo.com:3306/fec?useUnicode=yes&characterEncoding=UTF-8"
			, "edgarrw", "ELPmBwMa44"
			, "com.mysql.jdbc.Driver")
	}
	
	/**
	 * Checks the ImportHistory table to see if there should be a full or
	 * incremental update, then inserts a note that the update is beginning.
	 * 
	 * @return
	 */
	def noteStartUpdate() {
		getFullUpdate()
		historyNote('b', 'Starting update.', '')
	}
	
	/**
	 * 
	 * @param eventType ('b' = begin update, 's' = success, 'f' = fail)
	 * @param shortMesssage
	 * @param lm
	 * @return
	 */
	def historyNote(String eventType, String shortMessage, String lm) {
		db.executeUpdate("""INSERT INTO UpdateHistory (`date`, eventType, shortMessage, longMessage)
VALUES (now(), ${eventType}, ${shortMessage}, ${lm})""")
	}
	
	def update() {
		createStagingTables()
		checkDataFolder()
		// the cm and cm production tables are updated by updateCn and updateCm
		updateCn()
		updateCm()
		// these updates load data into staging tables
		updateContributions()
		moveStagingToProd()
		dropStagingTables()
		updateCMMeta()
		if (theresBeenAnError) {
			historyNote('f', 'There was an error updating data.', longMessage)
		} else {
			historyNote('s', 'Data updated', longMessage)
		}
		println "DONE."
	}
	
	/**
	 * If nothing has been imported in the previous week, we want to do
	 * a full update.
	 * @return
	 */
	boolean getFullUpdate() {
		String sql = """SELECT COUNT(id) AS c
FROM UpdateHistory
WHERE `date` > DATE_SUB(now(), INTERVAL 13 DAY)
AND eventType = 's'
AND id = (SELECT MAX(id) FROM UpdateHistory)"""
		int c = db.firstRow(sql).c
		println "last update update less than 13 days ago succeeded count = ${c}"
		fullUpdate = (c == 0)
	}
	
	def dropStagingTables() {
		db.execute("DROP TABLE IF EXISTS `scn`")
		db.execute("DROP TABLE IF EXISTS `scm`")
		db.execute("DROP TABLE IF EXISTS `sindiv`")
		db.execute("DROP TABLE IF EXISTS `sadd`")
		db.execute("DROP TABLE IF EXISTS `schg`")
		db.execute("DROP TABLE IF EXISTS `sdelete`")
		db.execute("DROP TABLE IF EXISTS `soth`")
		db.execute("DROP TABLE IF EXISTS `spas2`")
	}
	
	def createStagingTables() {
		dropStagingTables()
				println "Creating staging tables.."
		db.execute("""CREATE TABLE `scn` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'numeric id assigned by this db',
  `y2` char(2) NOT NULL DEFAULT 'xx' COMMENT 'year of the data which might not be the same as the year of ellection',
  `cn_id` char(9) NOT NULL DEFAULT '' COMMENT 'id assigned by FEC',
  `name` varchar(200) DEFAULT NULL,
  `party` varchar(3) DEFAULT NULL,
  `election_year` decimal(4,0) DEFAULT NULL,
  `office_state` char(2) DEFAULT NULL,
  `office` char(1) DEFAULT NULL,
  `district` char(2) DEFAULT NULL,
  `i_c_status` char(1) DEFAULT NULL,
  `cn_status` char(1) DEFAULT NULL,
  `cm_id` char(9) DEFAULT NULL,
  `street1` varchar(38) DEFAULT NULL,
  `street2` varchar(38) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  PRIMARY KEY (`cn_id`,`y2`),
  UNIQUE KEY `IX_id` (`id`),
  KEY `IX_cm_id` (`cm_id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Candidates';
""")
		
		db.execute("""CREATE TABLE `scm` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `y2` char(2) NOT NULL DEFAULT 'xx' COMMENT 'year of data which might not be the same as the year of the election',
  `cm_id` char(9) NOT NULL,
  `name` varchar(90) DEFAULT NULL,
  `treasurer` varchar(38) DEFAULT NULL,
  `street_1` varchar(34) DEFAULT NULL,
  `street_2` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `cm_designation` char(1) DEFAULT NULL,
  `cm_type` char(1) DEFAULT NULL,
  `party` char(3) DEFAULT NULL,
  `filing_freq` char(1) DEFAULT NULL,
  `interest_group_cat` char(1) DEFAULT NULL,
  `org_name` varchar(38) DEFAULT NULL,
  `cn_id` char(9) DEFAULT NULL,
  PRIMARY KEY (`cm_id`,`y2`),
  UNIQUE KEY `IX_id` (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `soth` (
  `y2` char(2) DEFAULT NULL,
  `cmte_id` char(9) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` char(8) DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_id` char(9) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `spas2` (
  `y2` char(2) DEFAULT NULL,
  `cmte_id` char(9) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` char(8) DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_id` char(9) DEFAULT NULL,
  `cand_id` char(9) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `sindiv` (
  `y2` char(2) DEFAULT NULL,
  `cmte_id` char(9) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` char(8) DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_id` char(9) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `sadd` (
  `y2` char(2) DEFAULT NULL,
  `cmte_id` char(9) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` char(8) DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_id` char(9) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `schg` (
  `y2` char(2) DEFAULT NULL,
  `cmte_id` char(9) DEFAULT NULL,
  `amndt_ind` char(1) DEFAULT NULL,
  `rpt_tp` varchar(3) DEFAULT NULL,
  `transaction_pgi` char(5) DEFAULT NULL,
  `image_num` varchar(11) DEFAULT NULL,
  `transaction_tp` varchar(3) DEFAULT NULL,
  `entity_tp` char(3) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `city` varchar(45) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip_code` varchar(9) DEFAULT NULL,
  `employer` varchar(45) DEFAULT NULL,
  `occupation` varchar(45) DEFAULT NULL,
  `transaction_dt` char(8) DEFAULT NULL,
  `transaction_amt` decimal(14,2) DEFAULT NULL,
  `other_id` char(9) DEFAULT NULL,
  `tran_id` varchar(45) DEFAULT NULL,
  `file_num` decimal(22,0) DEFAULT NULL,
  `memo_cd` char(1) DEFAULT NULL,
  `memo_text` varchar(100) DEFAULT NULL,
  `sub_id` decimal(19,0) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
""")
		db.execute("""CREATE TABLE `sdelete` (
  `sub_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`sub_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
	}
	
	def checkDataFolder(){
		File dataFolder = new File(".//data")
		if (!dataFolder.exists()) {
			dataFolder.mkdirs();
		}
	}
	
	/**
	 * Candidate and Committee tables are created from all available files in
	 * staging tables. Then any updates are inserted into the production
	 * tables.
	 * @return
	 */
	def updateCn() {
		File outFile = new File(".//data/cn.txt")
		def out = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(outFile), "UTF8"))
		dataFolder.eachFileRecurse { file ->
			Matcher matcher = file.name =~ /(?i)cn(\d\d).zip/
			if (matcher) {
				String y2 = matcher[0][1]
				println "${file.name} - ${y2}"
				def zipFile = new ZipFile(file)
				int recCount = 0
				zipFile.entries.each { zipFileEntry ->
					println "\tzip entry: ${zipFileEntry.name}"
					InputStream ins = zipFile.getInputStream(zipFileEntry)
					BufferedReader br = ins.newReader()
					br.eachLine { line ->
						out << "${y2}|${line}\n"
						recCount += 1
					}
					br.close()
				}
				zipFile.close()
				println"\t${recCount} lines processed into cn.txt"
			}
		}
		out.close()
		println "Loading cn.txt into scn table.."
		db.execute("TRUNCATE TABLE scn")
		int recCount = db.executeUpdate("""LOAD DATA LOCAL INFILE '""" + outFile.absolutePath 
			+ """' INTO TABLE scn
CHARACTER SET utf8
FIELDS TERMINATED BY '|'
(y2,cn_id,`name` ,party ,election_year, office_state, office
, district, i_c_status, cn_status, cm_id, street1, street2, city
, `state`, zip)""")
		println "${recCount} rows loaded. UPDATING any changed records.."
		recCount = db.executeUpdate("""UPDATE cn  
INNER JOIN scn ON scn.y2 = cn.y2 AND scn.cn_id = cn.cn_id
SET cn.`name` = scn.`name` , cn.party = scn.party
, cn.election_year = scn.election_year, cn.office_state = scn.office_state
, cn.office = scn.office, cn.district = scn.district
, cn.i_c_status = scn.i_c_status, cn.cn_status = scn.cn_status
, cn.cm_id = scn.cm_id, cn.street1 = scn.street1, cn.street2 = scn.street2
, cn.city = scn.city, cn.`state` = scn.`state`, cn.zip = scn.zip
WHERE (cn.`name` != scn.`name` OR cn.party != scn.party
OR cn.election_year != scn.election_year OR cn.office_state != scn.office_state
OR cn.office != scn.office OR cn.district != scn.district
OR cn.i_c_status != scn.i_c_status OR cn.cn_status != scn.cn_status
OR cn.cm_id != scn.cm_id OR cn.street1 != scn.street1 OR cn.street2 != scn.street2
OR cn.city != scn.city OR cn.`state` != scn.`state` OR cn.zip != scn.zip)
""")
		println "${recCount} updates. INSERTing new cn records into cn table.."
		longMessage += "${recCount} updated and "
		recCount = db.executeUpdate("""INSERT INTO cn (y2,cn_id,`name` ,party 
,election_year, office_state, office
, district, i_c_status, cn_status, cm_id, street1, street2, city
, `state`, zip)
SELECT scn.y2, scn.cn_id, scn.`name`, scn.party, scn.election_year
, scn.office_state, scn.office, scn.district, scn.i_c_status, scn.cn_status
, scn.cm_id, scn.street1, scn.street2, scn.city, scn.`state`, scn.zip
FROM scn
LEFT OUTER JOIN cn AS ocn ON scn.y2 = ocn.y2 AND scn.cn_id = ocn.cn_id
WHERE ocn.id IS NULL""")
		println ("${recCount} new cn records.")
		longMessage += "${recCount} new candidate records.\n"
		outFile.delete()
	}
	
	def updateCm() {
		File outFile = new File(".//data/cm.txt")
		def out = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(outFile), "UTF8"))
		dataFolder.eachFileRecurse { file ->
			Matcher matcher = file.name =~ /(?i)cm(\d\d).zip/
			if (matcher) {
				String y2 = matcher[0][1]
				println "${file.name} - ${y2}"
				//def zipFile = new java.util.zip.ZipFile(file)
				def zipFile = new ZipFile(file)
				int recCount = 0
				zipFile.entries.each { zipFileEntry ->
					println "\tzip entry: ${zipFileEntry.name}"
					InputStream ins = zipFile.getInputStream(zipFileEntry)
					BufferedReader br = ins.newReader()  // old file format needed newReader("ISO8859_1")
					br.eachLine { line ->
						out << "${y2}|${line}\n"
						recCount += 1
					}
				}
				zipFile.close()
				println"\t${recCount} lines processed into cm.txt"
			}
		}
		out.close()
		println "Loading cm.txt into scm table.."
		/**
y2, cm_id, `name`, treasurer, street_1, street_2, city, state, zip
, cm_designation, cm_type, party, filing_freq, interest_group_cat
, org_name, cn_id, `year`
		 */
		db.execute("TRUNCATE TABLE scm")
		int recCount = db.executeUpdate("""LOAD DATA LOCAL INFILE '""" + outFile.absolutePath
			+ """' INTO TABLE scm
CHARACTER SET utf8
FIELDS TERMINATED BY '|'
				(y2, cm_id, `name`, treasurer, street_1, street_2, city, state, zip
, cm_designation, cm_type, party, filing_freq, interest_group_cat
, org_name, cn_id)""")
		println "${recCount} rows loaded. UPDATING any changed records.."
		recCount = db.executeUpdate("""UPDATE cm  
INNER JOIN scm ON scm.y2 = cm.y2 AND scm.cm_id = cm.cm_id
SET cm.`name` = scm.`name`, cm.treasurer = scm.treasurer
, cm.street_1 = scm.street_1, cm.street_2 = scm.street_2
, cm.city = scm.city, cm.state = scm.state, cm.zip = scm.zip
, cm.cm_designation = scm.cm_designation, cm.cm_type = scm.cm_type
, cm.party = scm.party, cm.filing_freq = scm.filing_freq
, cm.interest_group_cat = scm.interest_group_cat
, cm.org_name = scm.org_name, cm.cn_id = scm.cn_id
WHERE (cm.`name` != scm.`name` OR cm.treasurer != scm.treasurer
OR cm.street_1 != scm.street_1 OR cm.street_2 != scm.street_2
OR cm.city != scm.city OR cm.state != scm.state OR cm.zip != scm.zip
OR cm.cm_designation != scm.cm_designation OR cm.cm_type != scm.cm_type
OR cm.party != scm.party OR cm.filing_freq != scm.filing_freq
OR cm.interest_group_cat != scm.interest_group_cat
OR cm.org_name != scm.org_name OR cm.cn_id != scm.cn_id)
""")
		println "${recCount} updates. INSERTing new cm records into cm table.."
		longMessage += "${recCount} updated and "
		recCount = db.executeUpdate("""INSERT INTO cm (y2, cm_id, `name`
, treasurer, street_1, street_2, city, state, zip
, cm_designation, cm_type, party, filing_freq, interest_group_cat
, org_name, cn_id)
SELECT scm.y2, scm.cm_id, scm.`name`, scm.treasurer, scm.street_1
, scm.street_2, scm.city, scm.state, scm.zip
, scm.cm_designation, scm.cm_type, scm.party, scm.filing_freq
, scm.interest_group_cat
, scm.org_name, scm.cn_id
FROM scm
LEFT OUTER JOIN cm AS ocm ON ocm.y2 = scm.y2 AND ocm.cm_id = scm.cm_id
WHERE ocm.id IS NULL
""")
		println ("${recCount} new cm records.")
		longMessage += "${recCount} new committee records.\n"
		outFile.delete()
	}
	
	def updateContributions() {
		File outFile = new File("./data/contrib.txt")
		def pat = (fullUpdate ? /(?i)(indiv|oth|pas2)(\d\d).zip/ : /(?i)(add|chg|delete|oth|pas2)(\d\d).zip/)
		println "${(fullUpdate ? 'full' : 'incremental')} update"
		dataFolder.eachFileRecurse { file ->
			Matcher matcher = file.name =~ pat
			if (matcher) {
				String fileType = matcher[0][1]
				String y2 = matcher[0][2]
				println "${file.name} - ${y2}"
				def zipFile = new ZipFile(file)
				int recCount = 0
				zipFile.entries.each { zipFileEntry ->
					println "\tzip entry: ${zipFileEntry.name}"
					InputStream ins = zipFile.getInputStream(zipFileEntry)
					BufferedReader br = ins.newReader() 
					def out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outFile), "UTF8"))
					br.eachLine { line ->
						out << "${line}\n"
						recCount += 1
					}
					println "Extracted ${recCount} lines. LOADing DATA.."
					out.close()
					ins.close()
					recCount = 0
					if (fileType.equals("oth")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE soth
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num
, transaction_tp, entity_tp, `name`, city, state
, zip_code, employer, occupation, transaction_dt
, transaction_amt, other_id, tran_id, file_num
, memo_cd, memo_text, sub_id)
""")
						db.executeUpdate("UPDATE soth SET y2 = ${y2} WHERE y2 IS NULL")
					} else if (fileType.equals("pas2")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE spas2
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num
, transaction_tp, entity_tp, `name`, city, state
, zip_code, employer, occupation, transaction_dt
, transaction_amt, other_id, cand_id, tran_id, file_num
, memo_cd, memo_text, sub_id)
""")
						db.executeUpdate("UPDATE spas2 SET y2 = ${y2} WHERE y2 IS NULL")
					} else if (fileType.equals("add")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE sadd
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num
, transaction_tp, entity_tp, `name`, city, state
, zip_code, employer, occupation, transaction_dt
, transaction_amt, other_id, tran_id, file_num
, memo_cd, memo_text, sub_id)
""")
						db.executeUpdate("UPDATE sadd SET y2 = ${y2} WHERE y2 IS NULL")
					} else if (fileType.equals("chg")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE schg
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num
, transaction_tp, entity_tp, `name`, city, state
, zip_code, employer, occupation, transaction_dt
, transaction_amt, other_id, tran_id, file_num
, memo_cd, memo_text, sub_id)
""")
						db.executeUpdate("UPDATE schg SET y2 = ${y2} WHERE y2 IS NULL")
					} else if (fileType.equals("delete")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE sdelete
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (sub_id)
""")
					} else if (fileType.equals("indiv")) {
						recCount = db.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ outFile.absolutePath + """' INTO TABLE sindiv
 CHARACTER SET utf8
 FIELDS TERMINATED BY '|'
 (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num
, transaction_tp, entity_tp, `name`, city, state
, zip_code, employer, occupation, transaction_dt
, transaction_amt, other_id, tran_id, file_num
, memo_cd, memo_text, sub_id)
""")
						db.executeUpdate("UPDATE sindiv SET y2 = ${y2} WHERE y2 IS NULL")
					}
					println "Loaded ${recCount} ${fileType} records"
				}
				zipFile.close()
			}
		}
		outFile.delete()
	}
	
	def moveStagingToProd() {
		Integer recCount
		// pas2 and oth are always full updates from staging tables
		println "TRUNCATing and INSERTING pas2"
		db.execute("TRUNCATE TABLE pas2")
		recCount = db.executeUpdate("""INSERT INTO pas2
(cm_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp
, entity_tp, `name`, city, state, zip_code, employer, occupation
, transaction_date, transaction_amt, other_cm_id, other_cn_id
, cn_id, tran_id, file_num, memo_cd, memo_text, sub_id)
SELECT
	cm.id AS cm_id
	, i.amndt_ind, i.rpt_tp, i.transaction_pgi, i.image_num, i.transaction_tp
	, i.entity_tp, i.`name`, i.city, i.state, i.zip_code, i.employer, i.occupation
	, CAST(CONCAT(
		RIGHT(i.transaction_dt, 4),'-', 
		LEFT(i.transaction_dt, 2), '-', 
		SUBSTRING(i.transaction_dt, 3, 2)) AS DATE) AS transaction_dt
	, i.transaction_amt
	, other_cm.id AS other_cm_id
	, other_cn.id AS other_cn_id
	, cn.id AS cn_id
	, i.tran_id, i.file_num, i.memo_cd, i.memo_text, i.sub_id
FROM spas2 AS i
LEFT OUTER JOIN cm ON i.cmte_id = cm.cm_id AND i.y2 = cm.y2
LEFT OUTER JOIN cn ON i.cand_id = cn.cn_id AND i.y2 = cn.y2
LEFT OUTER JOIN cm AS other_cm ON i.other_id = other_cm.cm_id AND i.y2 = other_cm.y2
LEFT OUTER JOIN cn AS other_cn ON i.other_id = other_cn.cn_id AND i.y2 = other_cn.y2
WHERE i.sub_id != 0""")
		println "${recCount} records inserted into pas2. TRUNCATing and INSERTing oth.."
		longMessage += "${recCount} total records inserted into pas2.\n"
		db.execute("TRUNCATE TABLE oth")
		recCount = db.executeUpdate("""INSERT INTO oth
(cm_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp
, entity_tp, `name`, city, state, zip_code, employer, occupation
, transaction_dt, transaction_amt, other_cm_id, other_cn_id
, tran_id, file_num, memo_cd, memo_text, sub_id)
SELECT
	cm.id AS cm_id
	, i.amndt_ind, i.rpt_tp, i.transaction_pgi, i.image_num, i.transaction_tp
	, i.entity_tp, i.`name`, i.city, i.state, i.zip_code, i.employer, i.occupation
	, CAST(CONCAT(
		RIGHT(i.transaction_dt, 4),'-', 
		LEFT(i.transaction_dt, 2), '-', 
		SUBSTRING(i.transaction_dt, 3, 2)) AS DATE) AS transaction_dt
	, i.transaction_amt
	, other_cm.id AS other_cm_id
	, other_cn.id AS other_cn_id
	, i.tran_id, i.file_num, i.memo_cd, i.memo_text, i.sub_id
FROM soth AS i
LEFT OUTER JOIN cm ON i.cmte_id = cm.cm_id AND cm.y2 = i.y2
LEFT OUTER JOIN cm AS other_cm ON i.other_id = other_cm.cm_id AND i.y2 = other_cm.y2
LEFT OUTER JOIN cn AS other_cn ON i.other_id = other_cn.cn_id AND i.y2 = other_cn.y2
WHERE i.sub_id != 0""")
		println "${recCount} records inserted."
		longMessage += "${recCount} total records inserted into oth.\n"
		// indiv is either a full insert from sindiv, or is updated from sadd,
		// schg, and sdelete.
		if (fullUpdate) {
			println "TRUNCATE indiv"
			db.execute("TRUNCATE TABLE indiv")
			println "INSERTing into indiv.."
			recCount = db.executeUpdate("""INSERT INTO indiv
(cm_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp
, entity_tp, name, city, state, zip_code, employer, occupation
, transaction_dt, transaction_amt, other_cm_id, other_cn_id
, tran_id, file_num, memo_cd, memo_text, sub_id)
SELECT
	cm.id AS cm_id
	, i.amndt_ind, i.rpt_tp, i.transaction_pgi, i.image_num, i.transaction_tp
	, i.entity_tp, i.`name`, i.city, i.state, i.zip_code, i.employer, i.occupation
	, CAST(CONCAT(
		RIGHT(i.transaction_dt, 4),'-', 
		LEFT(i.transaction_dt, 2), '-', 
		SUBSTRING(i.transaction_dt, 3, 2)) AS DATE) AS transaction_dt
	, i.transaction_amt
	, other_cm.id AS other_cm_id
	, other_cn.id AS other_cn_id
	, i.tran_id, i.file_num, i.memo_cd, i.memo_text, i.sub_id
FROM sindiv AS i
LEFT OUTER JOIN cm ON i.cmte_id = cm.cm_id AND cm.y2 = i.y2
LEFT OUTER JOIN cm AS other_cm ON i.other_id = other_cm.cm_id AND i.y2 = other_cm.y2
LEFT OUTER JOIN cn AS other_cn ON i.other_id = other_cn.cn_id AND i.y2 = other_cn.y2
WHERE i.sub_id != 0
""")
			println "${recCount} records inserted into indiv."
			longMessage += "${recCount} total records inserted into indiv for full update.\n"
		} else { //incremental update
			println "DELETing from indiv.."
			recCount = db.executeUpdate("""DELETE FROM indiv 
WHERE sub_id IN (SELECT sub_id FROM sdelete)""")
			println "${recCount} rows deleted from indiv. INSERTing adds.."
			longMessage += "${recCount} deletions, "
			recCount = db.executeUpdate("""REPLACE INTO indiv
(cm_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp
, entity_tp, name, city, state, zip_code, employer, occupation
, transaction_dt, transaction_amt, other_cm_id, other_cn_id
, tran_id, file_num, memo_cd, memo_text, sub_id)
SELECT
	cm.id AS cm_id
	, i.amndt_ind, i.rpt_tp, i.transaction_pgi, i.image_num, i.transaction_tp
	, i.entity_tp, i.`name`, i.city, i.state, i.zip_code, i.employer, i.occupation
	, CAST(CONCAT(
		RIGHT(i.transaction_dt, 4),'-', 
		LEFT(i.transaction_dt, 2), '-', 
		SUBSTRING(i.transaction_dt, 3, 2)) AS DATE) AS transaction_dt
	, i.transaction_amt
	, other_cm.id AS other_cm_id
	, other_cn.id AS other_cn_id
	, i.tran_id, i.file_num, i.memo_cd, i.memo_text, i.sub_id
FROM sadd AS i
LEFT OUTER JOIN cm ON i.cmte_id = cm.cm_id AND cm.y2 = i.y2
LEFT OUTER JOIN cm AS other_cm ON i.other_id = other_cm.cm_id AND i.y2 = other_cm.y2
LEFT OUTER JOIN cn AS other_cn ON i.other_id = other_cn.cn_id AND i.y2 = other_cn.y2
WHERE i.sub_id != 0""")
			println "${recCount} records added. REPLACing changes.."
			longMessage += "${recCount} additions, and "
			recCount = db.executeUpdate("""REPLACE INTO indiv
(cm_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp
, entity_tp, name, city, state, zip_code, employer, occupation
, transaction_dt, transaction_amt, other_cm_id, other_cn_id
, tran_id, file_num, memo_cd, memo_text, sub_id)
SELECT
	cm.id AS cm_id
	, i.amndt_ind, i.rpt_tp, i.transaction_pgi, i.image_num, i.transaction_tp
	, i.entity_tp, i.`name`, i.city, i.state, i.zip_code, i.employer, i.occupation
	, CAST(CONCAT(
		RIGHT(i.transaction_dt, 4),'-', 
		LEFT(i.transaction_dt, 2), '-', 
		SUBSTRING(i.transaction_dt, 3, 2)) AS DATE) AS transaction_dt
	, i.transaction_amt
	, other_cm.id AS other_cm_id
	, other_cn.id AS other_cn_id
	, i.tran_id, i.file_num, i.memo_cd, i.memo_text, i.sub_id
FROM schg AS i
LEFT OUTER JOIN cm ON i.cmte_id = cm.cm_id AND cm.y2 = i.y2
LEFT OUTER JOIN cm AS other_cm ON i.other_id = other_cm.cm_id AND i.y2 = other_cm.y2
LEFT OUTER JOIN cn AS other_cn ON i.other_id = other_cn.cn_id AND i.y2 = other_cn.y2
WHERE i.sub_id != 0""")
			println "${recCount} records changed."
			longMessage += "${recCount} changes in indiv incremental update.\n"
		}
	}
	
	def updateCMMeta() {
		db.executeUpdate("TRUNCATE TABLE cmMeta;")
		db.executeUpdate("""INSERT INTO cmMeta (cm_id, contribSum, contribCount)
SELECT i.cm_id, sum(i.transaction_amt), COUNT(i.sub_id)
FROM indiv AS i
WHERE i.cm_id IS NOT NULL
GROUP BY i.cm_id""")
	}
	
}
