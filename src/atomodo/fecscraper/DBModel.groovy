package atomodo.fecscraper

import groovy.sql.Sql
import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.Period
import org.joda.time.format.PeriodFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

class DBModel {
	
	private File dataFolder
	private static DateTimeFormatter dateTimeFormatter
	private static Calendar calendar
	private String lastGoodDate = "1990/01/03"
	private String lastFileType = null
	def db
	Boolean fullUpdate = false
	
	public DBModel(File dataFolder) {
		this()
		this.dataFolder = dataFolder
	}
	
	public DBModel() {
		db = Sql.newInstance("jdbc:mysql://www.atomodo.com:3306/edgar?useUnicode=yes&characterEncoding=UTF-8"
			, "edgarrw"
			, "ELPmBwMa44"
			, "com.mysql.jdbc.Driver")
		dateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd")
	}
	
	def update() {
		DateTime startTime = new DateTime()
		Boolean somethingChanged = false
		fullUpdate = checkForFullUpdate() // A full update replaces IndividualContributions
		// with the data in the indivXX files (this takes hours). A differential
		// update uses the addXX chgXX deleteXX files.
		def dataFiles = []
		dataFolder.eachFile { file ->
			if (file.name.toLowerCase().endsWith('.zip')
				&& (file.name =~ FECDataFile.namePattern)
				&& ((!fullUpdate && !file.name.toLowerCase().startsWith('indiv'))
					|| (fullUpdate && !(file.name.toLowerCase().startsWith('add')
						|| file.name.toLowerCase().startsWith('chg') 
						|| file.name.toLowerCase().startsWith('delete')))) ){
					FECDataFile fecDataFile = new FECDataFile()
					fecDataFile.zipFile = file
					dataFiles << fecDataFile
				}				
		}
		dataFiles.each { fdf ->
			print "${fdf.zipFile.name}.."
			String sql = "SELECT fileLength, id FROM ImportHistory WHERE fileName LIKE '${fdf.zipFile.name}'"
			def row = db.firstRow(sql)
			boolean update = false
			if (!row) {
				println "..new file.."
				db.execute("INSERT INTO ImportHistory (fileName, fileLength, dateRetrieved, y2) VALUES (${fdf.zipFile.name}, ${fdf.zipFile.length()}, NOW(), ${fdf.y2})")
				row = db.firstRow(sql)
				update = true
			}
			if (!update && row && row.fileLength == fdf.zipFile.length() && fdf.csvFile.canRead()){
				println ".. not changed."
			} else {
				if (row.fileLength != fdf.zipFile.length()) {
					println ".. file changed. Old = ${row.fileLength}, new = ${fdf.zipFile.length()} .."
				} else if (!fdf.csvFile.canRead()) {
					println "creating new csv file.."
				}
				fdf.importHistoryId = row.id
				somethingChanged = true
				printElapsedTime(startTime)
				Integer recCount = writeCsvFile(fdf)
				db.execute("""UPDATE ImportHistory SET dateParsed = NOW()
					, recordCount = ${recCount}, fileLength = ${fdf.zipFile.length()}
					WHERE id = ${fdf.importHistoryId}""")
			}			
		}
		if (somethingChanged) {
			createStagingTables()
			disableKeys()
			lastFileType = null
			dataFiles.each { fdf ->
				printElapsedTime(startTime)
				loadDataFile(fdf)
			}
			printElapsedTime(startTime)
			enableKeys()
			printElapsedTime(startTime)
			moveStagingToProd()
			printElapsedTime(startTime)
		} else {
			println "no changes."
		}

	}

	/**
	 * If it has been more than 10 days since the add chg or delete
	 * files have been processed, or if there is an indiv file that
	 * has never been processed, then we should do a full update
	 * which replaces the IndividualContributions table with the
	 * data in the indiv files.
	 * Also, if IndividualContributions is empty, we start from 
	 * indiv.
	 * @return
	 */
	Boolean checkForFullUpdate() {
		Integer d = db.firstRow("""SELECT DATEDIFF(NOW(), MAX(dateParsed)) AS d
FROM ImportHistory
WHERE fileName LIKE 'add%' OR fileName LIKE 'chg%'
    OR fileName LIKE 'delete%' OR fileName LIKE 'indiv%'""").d
		if (d != null && d > 10) {
			println "It has been $d days since the last update, so we'll do a full."
			return true
		}
		d = db.firstRow("""SELECT COUNT(*) AS d
FROM ImportHistory
WHERE fileName LIKE 'indiv%' AND dateParsed IS NULL""").d
		if (d > 0) {
			println "There is a new indiv file, so we'll do a full update."
			return true
		}
		d = db.firstRow("""SELECT COUNT(*) AS d
FROM IndividualContributions""").d
		if (d == 0) {
			println "The IndividualContributions table is empty, so we'll do a full update."
			return true
		}		
		println "Doing a differential update."
		return false
	}
	
	def printElapsedTime(DateTime startTime) {
		Period p = new Period(startTime, new DateTime())
		println "    ${PeriodFormat.getDefault().print(p)} elapsed"
	}
	
	Integer writeCsvFile(FECDataFile fecDataFile) {
		Integer recCount = 0
		if (!fecDataFile.fileType) {
			println "file not recognised."
			return recCount
		}
		def lineParser = null
		if (fecDataFile.fileType == "cm"){
//			insSql = """REPLACE INTO Committees
//							(id, name, treasurer
//							, street1, street2, city
//							, state, zip, designation
//							, type, party, filingFrequency
//							, interestGroupCategory, connectedOrganizationName, candidateId)
//						VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
			lineParser = { line ->
				def v = [line[0..8], "${fecDataFile.y2}", line[9..98].trim(), line[99..136].trim()
							, line[137..170].trim(), line[171..204].trim(), line[205..222].trim()
							, line[223..224].trim(), line[225..229].trim(), line[230].trim()
							, line[231].trim(), line[232..234].trim(), line[235].trim()
							, line[236].trim(), line[237..274].trim(), line[275..283].trim()]
				return v.collect { it.replaceAll("\t", "    ").replaceAll("\\\\", "\\\\\\\\") } .join("\t")
			}
		} else if (fecDataFile.fileType == "cn"){
//			insSql = """REPLACE INTO Candidates
//						(id, name, party1
//						, party3, incumb, candidateStatus
//						, street1, street2, city
//						, state, zip, principalCampaignCommId
//						, yearOfElection, currentDistrict) VALUES
//						(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
			lineParser = { line ->
				def v = [line[0..8], "${fecDataFile.y2}", line[9..46].trim(), line[47..49].trim()
							, line[53..55].trim(), line[56].trim(), line[58].trim()
							, line[59..92].trim(), line[93..126].trim(), line[127..144].trim()
							, line[145..146].trim(), line[147..151].trim(), line[152..160].trim()
							, line[161..162].trim(), line[163..164].trim()]
				return v.collect { it.replaceAll("\t", "    ").replaceAll("\\\\", "\\\\\\\\") } .join("\t")
			}
		} else if (fecDataFile.fileType == "pas2"){
//			insSql = """INSERT INTO CommitteeContributions
//							(id, y2
//							, filerId, amendment, reportType
//							, pgi, microfilmLoc, transactionType
//							, transactionDate
//							, amount, oid, candidateId)
//						VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
			lineParser = { line ->
				def v = [line[61..67], "${fecDataFile.y2}"
							, line[0..8], line[9].trim(), line[10..12].trim()
							, line[13].trim(), line[14..24].trim(), line[25..27].trim()
							, checkDate(line[32..35]+"/"+line[28..29]+"/"+line[30..31])
							, "${parseCOBOLAmount(line[36..42].trim())}", line[43..51].trim(), line[52..60].trim()]
				return v.collect { it.replaceAll("\t", " ").replaceAll("\\\\", "\\\\\\\\") } .join("\t")
			}
		} else if (fecDataFile.fileType == "indiv"
			|| fecDataFile.fileType == "add"
			|| fecDataFile.fileType == "chg"){
//			insSql = """INSERT INTO IndividualContributions
//							(id, y2
//							, filerId, amendment, reportType
//							, pgi, microfilmLoc, name
//							, city, state, zip
//							, occupation, transactionDate, amount
//							, oid, transactionType) VALUES
//							(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
			lineParser = { line ->
				def v = [line[146..152], "${fecDataFile.y2}"
							, line[0..8].trim(), line[9].trim(), line[10..12].trim()
							, line[13].trim(), line[14..24].trim(), line[28..61].trim()
							, line[62..79].trim(), line[80..81].trim(), line[82..86].trim()
							, line[87..121].trim(), checkDate(line[126..129]+"/"+line[122..123]+"/"+line[124..125])
							, "${parseCOBOLAmount(line[130..136].trim())}"
							, line[137..145].trim(), line[25..27].trim()]
				return v.collect { it.replaceAll("\t", " ").replaceAll("\\\\", "\\\\\\\\") } .join("\t")
			}
		} else if (fecDataFile.fileType == "oth"){
//			insSql = """INSERT INTO OtherContributions
//							(id, y2
//							, filerId, amendment, reportType
//							, pgi, microfilmLoc, name
//							, city, state, zip
//							, occupation, transactionDate, amount
//							, oid, transactionType) VALUES
//							(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
			lineParser = { line ->
				def v = [line[146..152], "${fecDataFile.y2}"
							, line[0..8].trim(), line[9].trim(), line[10..12].trim()
							, line[13].trim(), line[14..24].trim(), line[28..61].trim()
							, line[62..79].trim(), line[80..81].trim(), line[82..86].trim()
							, line[87..121].trim(), checkDate(line[126..129]+"/"+line[122..123]+"/"+line[124..125])
							, "${parseCOBOLAmount(line[130..136].trim())}"
							, line[137..145].trim(), line[25..27].trim()]
				return v.collect { it.replaceAll("\t", "    ").replaceAll("\\\\", "\\\\\\\\") } .join("\t")
			}
		} else if (fecDataFile.fileType == "delete") {
			lineParser = { line ->
				return "${line}\t${fecDataFile.y2}"
			}
		} else {
			println "file (${fecDataFile.csvFile.name}) matched, but didn't fit a type - very weird!"
			return 0
		}

		def out = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(fecDataFile.csvFile), "UTF8"))
		def zipFile = new java.util.zip.ZipFile(fecDataFile.zipFile)
		zipFile.entries().findAll { it.name.endsWith(".dta") }.each { zipFileEntry ->
			println "\tzip entry: ${zipFileEntry.name}"
			InputStream ins = zipFile.getInputStream(zipFileEntry)
			BufferedReader br = ins.newReader("ISO8859_1")
			def rows = [:]
			int c = 0
			br.eachLine { line ->
				out << "${lineParser(line)}\n"
				recCount += 1
			}
		}
		out.close()
		println"\t${recCount} lines processed into ${fecDataFile.csvFile.name}"
		return recCount
	}
	
	def createStagingTables(){
		println "Creating staging tables.."
		db.execute("DROP TABLE IF EXISTS `sCandidates`")
		db.execute("DROP TABLE IF EXISTS `sCommitteeContributions`")
		db.execute("DROP TABLE IF EXISTS `sCommittees`")
		db.execute("DROP TABLE IF EXISTS `sIndividualContributions`")
		db.execute("DROP TABLE IF EXISTS `sAdds`")
		db.execute("DROP TABLE IF EXISTS `sChgs`")
		db.execute("DROP TABLE IF EXISTS `sDeletes`")
		db.execute("DROP TABLE IF EXISTS `sOtherContributions`")
		db.execute("""CREATE TABLE `sCandidates` (
  `id` char(9) NOT NULL DEFAULT '',
  `y2` tinyint unsigned NOT NULL,
  `name` varchar(38) DEFAULT NULL,
  `party1` varchar(3) DEFAULT NULL,
  `party3` varchar(3) DEFAULT NULL,
  `incumb` char(1) DEFAULT NULL,
  `candidateStatus` char(1) DEFAULT NULL,
  `street1` varchar(34) DEFAULT NULL,
  `street2` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `principalCampaignCommId` char(9) DEFAULT NULL,
  `yearOfElection` char(2) DEFAULT NULL,
  `currentDistrict` char(2) DEFAULT NULL,
  PRIMARY KEY (`id`, `y2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		
		db.execute("""CREATE TABLE `sCommitteeContributions` (
  `id` int(10) unsigned NOT NULL DEFAULT '0',
  `y2` tinyint unsigned NOT NULL,
  `filerId` char(9) DEFAULT NULL,
  `amendment` char(1) NOT NULL,
  `reportType` char(3) DEFAULT NULL,
  `pgi` char(1) DEFAULT NULL,
  `microfilmLoc` char(11) DEFAULT NULL,
  `transactionType` varchar(3) NOT NULL,
  `transactionDate` date DEFAULT NULL,
  `amount` decimal(7,0) DEFAULT NULL,
  `oid` char(9) DEFAULT NULL,
  `candidateId` char(9) DEFAULT NULL,
  PRIMARY KEY (`id`,`y2`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sCommittees` (
  `id` char(9) NOT NULL,
  `y2` tinyint unsigned NOT NULL,
  `name` varchar(90) DEFAULT NULL,
  `treasurer` varchar(38) DEFAULT NULL,
  `street1` varchar(34) DEFAULT NULL,
  `street2` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `designation` char(1) DEFAULT NULL,
  `type` char(1) DEFAULT NULL,
  `party` varchar(3) DEFAULT NULL,
  `filingFrequency` char(1) DEFAULT NULL,
  `interestGroupCategory` char(1) DEFAULT NULL,
  `connectedOrganizationName` varchar(38) DEFAULT NULL,
  `candidateId` char(9) DEFAULT NULL,
  PRIMARY KEY (`id`, `y2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sIndividualContributions` (
  `id` int(10) unsigned NOT NULL DEFAULT '0',
  `y2` tinyint  unsigned NOT NULL DEFAULT '0',
  `filerId` char(9) DEFAULT NULL,
  `amendment` char(1) DEFAULT NULL,
  `reportType` char(3) DEFAULT NULL,
  `pgi` char(1) DEFAULT NULL,
  `microfilmLoc` char(11) DEFAULT NULL,
  `name` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `occupation` varchar(35) DEFAULT NULL,
  `transactionDate` date DEFAULT NULL,
  `amount` decimal(7,0) DEFAULT NULL,
  `oid` char(9) DEFAULT NULL,
  `transactionType` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`id`,`y2`) USING BTREE,
  KEY `ix_transactionDate` (`transactionDate`),
  KEY `ix_filerId` (`filerId`),
  KEY `ix_state` (`state`),
  KEY `ix_zip` (`zip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sAdds` (
  `id` int(10) unsigned NOT NULL DEFAULT '0',
  `y2` tinyint unsigned NOT NULL DEFAULT '0',
  `filerId` char(9) DEFAULT NULL,
  `amendment` char(1) DEFAULT NULL,
  `reportType` char(3) DEFAULT NULL,
  `pgi` char(1) DEFAULT NULL,
  `microfilmLoc` char(11) DEFAULT NULL,
  `name` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `occupation` varchar(35) DEFAULT NULL,
  `transactionDate` date DEFAULT NULL,
  `amount` decimal(7,0) DEFAULT NULL,
  `oid` char(9) DEFAULT NULL,
  `transactionType` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`id`,`y2`) USING BTREE,
  KEY `ix_transactionDate` (`transactionDate`),
  KEY `ix_filerId` (`filerId`),
  KEY `ix_state` (`state`),
  KEY `ix_zip` (`zip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sChgs` (
  `id` int(10) unsigned NOT NULL DEFAULT '0',
  `y2` tinyint unsigned NOT NULL DEFAULT '0',
  `filerId` char(9) DEFAULT NULL,
  `amendment` char(1) DEFAULT NULL,
  `reportType` char(3) DEFAULT NULL,
  `pgi` char(1) DEFAULT NULL,
  `microfilmLoc` char(11) DEFAULT NULL,
  `name` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `occupation` varchar(35) DEFAULT NULL,
  `transactionDate` date DEFAULT NULL,
  `amount` decimal(7,0) DEFAULT NULL,
  `oid` char(9) DEFAULT NULL,
  `transactionType` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`id`,`y2`) USING BTREE,
  KEY `ix_transactionDate` (`transactionDate`),
  KEY `ix_filerId` (`filerId`),
  KEY `ix_state` (`state`),
  KEY `ix_zip` (`zip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sDeletes` (
  `id` int(10) unsigned NOT NULL,
  `y2` tinyint unsigned NOT NULL,
  PRIMARY KEY (`id`,`y2`) 
)ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
		db.execute("""CREATE TABLE `sOtherContributions` (
  `id` int(10) unsigned NOT NULL,
  `y2` tinyint unsigned NOT NULL,
  `filerId` char(9) DEFAULT NULL,
  `amendment` char(1) DEFAULT NULL,
  `reportType` varchar(3) DEFAULT NULL,
  `pgi` char(1) DEFAULT NULL,
  `microfilmLoc` char(11) DEFAULT NULL,
  `transactionType` varchar(3) DEFAULT NULL,
  `name` varchar(34) DEFAULT NULL,
  `city` varchar(18) DEFAULT NULL,
  `state` char(2) DEFAULT NULL,
  `zip` char(5) DEFAULT NULL,
  `occupation` varchar(35) DEFAULT NULL,
  `transactionDate` date DEFAULT NULL,
  `amount` decimal(7,0) DEFAULT NULL,
  `oid` char(9) DEFAULT NULL,
  PRIMARY KEY (`id`,`y2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
""")
	}
	
	def disableKeys(){
		db.execute("ALTER TABLE `sCandidates` DISABLE KEYS")
		db.execute("ALTER TABLE `sCommitteeContributions` DISABLE KEYS")
		db.execute("ALTER TABLE `sCommittees` DISABLE KEYS")
		db.execute("ALTER TABLE `sIndividualContributions` DISABLE KEYS")
		db.execute("ALTER TABLE `sAdds` DISABLE KEYS")
		db.execute("ALTER TABLE `sChgs` DISABLE KEYS")
		db.execute("ALTER TABLE `sDeletes` DISABLE KEYS")
		db.execute("ALTER TABLE `sOtherContributions` DISABLE KEYS")
	}
	
	def enableKeys(){
		println "Enabling keys.."
		db.execute("ALTER TABLE `sCandidates` ENABLE KEYS")
		db.execute("ALTER TABLE `sCommitteeContributions` ENABLE KEYS")
		db.execute("ALTER TABLE `sCommittees` ENABLE KEYS")
		db.execute("ALTER TABLE `sIndividualContributions` ENABLE KEYS")
		db.execute("ALTER TABLE `sAdds` ENABLE KEYS")
		db.execute("ALTER TABLE `sChgs` ENABLE KEYS")
		db.execute("ALTER TABLE `sDeletes` ENABLE KEYS")
		db.execute("ALTER TABLE `sOtherContributions` ENABLE KEYS")
	}

	def moveStagingToProd() {
		println "Moving staging tables to production.."
		db.connection.autoCommit = false
		db.execute("DROP TABLE Candidates")
		db.execute("DROP TABLE Committees")
		db.execute("DROP TABLE CommitteeContributions")
		db.execute("DROP TABLE OtherContributions")
		db.execute("ALTER TABLE sCandidates RENAME TO Candidates")
		db.execute("ALTER TABLE sCommittees RENAME TO Committees")
		db.execute("ALTER TABLE sCommitteeContributions RENAME TO CommitteeContributions")
		db.execute("ALTER TABLE sOtherContributions RENAME TO OtherContributions")
		if (fullUpdate) {
			db.execute("DROP TABLE IndividualContributions")
			db.execute("ALTER TABLE sIndividualContributions RENAME TO IndividualContributions")
		} else {
			println "Deleting from IndividualContributions..."
			int c = 0;
			c = db.executeUpdate("""DELETE IndividualContributions
FROM IndividualContributions
JOIN sDeletes ON sDeletes.id = IndividualContributions.id AND sDeletes.y2 = IndividualContributions.y2
WHERE sDeletes.id IS NOT NULL""")
			println "${c} rows affected."
			println "Processing changes to IndividualContributions..."
			c = db.executeUpdate("""INSERT INTO IndividualContributions (id, y2
, filerId, amendment, reportType
, pgi, microfilmLoc, `name`
, city, state, zip
, occupation, transactionDate, amount
, oid, transactionType)
SELECT id, y2
, filerId, amendment, reportType
, pgi, microfilmLoc, `name`
, city, state, zip
, occupation, transactionDate, amount
, oid, transactionType FROM sChgs ON DUPLICATE KEY 
UPDATE filerId = VALUES(filerId), amendment = VALUES(amendment), reportType = VALUES(reportType)
, pgi = VALUES(pgi), microfilmLoc = VALUES(microfilmLoc), `name` = VALUES(`name`)
, city = VALUES(city), state = VALUES(state), zip = VALUES(zip)
, occupation = VALUES(occupation), transactionDate = VALUES(transactionDate), amount = VALUES(amount)
, oid = VALUES(oid), transactionType = VALUES(transactionType)""")
			println "${c} rows affected."
			println "Processing adds to IndividualContributions..."
			c = db.executeUpdate("""INSERT INTO IndividualContributions (id, y2
, filerId, amendment, reportType
, pgi, microfilmLoc, `name`
, city, state, zip
, occupation, transactionDate, amount
, oid, transactionType)
SELECT id, y2
, filerId, amendment, reportType
, pgi, microfilmLoc, `name`
, city, state, zip
, occupation, transactionDate, amount
, oid, transactionType FROM sAdds ON DUPLICATE KEY 
UPDATE filerId = VALUES(filerId), amendment = VALUES(amendment), reportType = VALUES(reportType)
, pgi = VALUES(pgi), microfilmLoc = VALUES(microfilmLoc), `name` = VALUES(`name`)
, city = VALUES(city), state = VALUES(state), zip = VALUES(zip)
, occupation = VALUES(occupation), transactionDate = VALUES(transactionDate), amount = VALUES(amount)
, oid = VALUES(oid), transactionType = VALUES(transactionType)""")
			println "${c} rows affected."
			db.execute("DROP TABLE IF EXISTS `sIndividualContributions`")
		}
		db.execute("DROP TABLE sAdds")
		db.execute("DROP TABLE sChgs")
		db.execute("DROP TABLE sDeletes")
		db.connection.commit()
		db.connection.autoCommit = true
// InnoDB doesn't support fulltext
//		db.execute("""ALTER TABLE `Candidates`
//ADD FULLTEXT INDEX `IX_Candidates_Name` (`name` ASC)""")
		
	}
	
	def loadDataFile(FECDataFile fdf) {
		if (!fdf.fileType) {
			println "file not recognised."
			return null
		}
		String insSql = ''
		String filePath = fdf.csvFile.canonicalFile.toString().replaceAll("\\\\", "\\\\\\\\")
		if (fdf.fileType == "cm"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sCommittees
							CHARACTER SET utf8
							(id, y2, name, treasurer
							, street1, street2, city
							, state, zip, designation
							, type, party, filingFrequency
							, interestGroupCategory, connectedOrganizationName, candidateId)"""
		} else if (fdf.fileType == "cn"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sCandidates
							CHARACTER SET utf8
						(id, y2, name, party1
						, party3, incumb, candidateStatus
						, street1, street2, city
						, state, zip, principalCampaignCommId
						, yearOfElection, currentDistrict)"""
		} else if (fdf.fileType == "pas2"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sCommitteeContributions
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2
							, filerId, amendment, reportType
							, pgi, microfilmLoc, transactionType
							, transactionDate
							, amount, oid, candidateId)"""
		} else if (fdf.fileType == "indiv"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sIndividualContributions
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2
							, filerId, amendment, reportType
							, pgi, microfilmLoc, name
							, city, state, zip
							, occupation, transactionDate, amount
							, oid, transactionType)"""
		} else if (fdf.fileType == "add"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sAdds
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2
							, filerId, amendment, reportType
							, pgi, microfilmLoc, name
							, city, state, zip
							, occupation, transactionDate, amount
							, oid, transactionType)"""
		} else if (fdf.fileType == "chg"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sChgs
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2
							, filerId, amendment, reportType
							, pgi, microfilmLoc, name
							, city, state, zip
							, occupation, transactionDate, amount
							, oid, transactionType)"""
		} else if (fdf.fileType == "delete"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sDeletes
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2)"""
		} else if (fdf.fileType == "oth"){
			insSql = """LOAD DATA LOCAL INFILE '""" + filePath + """' REPLACE INTO TABLE sOtherContributions
							CHARACTER SET utf8
							FIELDS TERMINATED BY '|'
							(id, y2
							, filerId, amendment, reportType
							, pgi, microfilmLoc, name
							, city, state, zip
							, occupation, transactionDate, amount
							, oid, transactionType)"""
		} else {
			println "file (${fdf.csvFile.name}) matched, but didn't fit a type - very weird!"
			return 0
		}
		println "Starting bulk load of ${fdf.csvFile.name}"
		db.execute(insSql)
	}
		
	/** 
	 * In the fixed width text file, the amounts are in COBOL format.  
	 * If the value is negative, the right most column will contain a 
	 * special character:  ] = -0, j = -1, k = -2, l = -3, m = -4, n = -5
	 * , o = -6, p = -7, q = -8, and r = -9.
	 * @param str String to parse as a COBOL formatted number
	 * @return Integer 
	 */
	Integer parseCOBOLAmount(String str) {
		Integer ret = null
		try {
			ret = Integer.parseInt(str)
		} catch (NumberFormatException ex) {
			def specialCs = [']', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r']
			int idx = specialCs.indexOf(str[str.size() - 1].toLowerCase())
			if (idx > -1) {
				str = "-${str[0..str.size() - 2]}${idx}"
				ret = Integer.parseInt(str)
			} else {
				println "\nammount not parsed- '${str}'"
				ret = 0
			}
		}
		return ret
	}
	
	private String checkDate(String dateStr) {
		String goodDate = dateStr
		try {
			DateTime dt = dateTimeFormatter.parseDateTime(dateStr)
		} catch (IllegalArgumentException ex) {
			goodDate = lastGoodDate
		}
		lastGoodDate = goodDate
		return goodDate
	}

}
