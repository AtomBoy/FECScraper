package atomodo.fecscraper

class FECDataFile {
	File zipFile = null
	File csvFile = null
	static final String namePattern = /(?i)(cm|cn|pas2|indiv|add|chg|delete|oth)(\d\d)(\.zip|\.csv)/
	String fileType = null
	Integer y2 = null
	Integer importHistoryId = null
	
	def FECDataFile(){
		
	}
	
	def setZipFile(File zipFile) {
		this.zipFile = zipFile
		def matcher = zipFile.name =~ namePattern
		if (matcher) {
			fileType = matcher[0][1]
			y2 = Integer.parseInt(matcher[0][2])
			String csvFileName = "${zipFile.path[0..zipFile.path.lastIndexOf('.')]}csv"
			csvFile = new File(csvFileName)
		}
	}
	
	def setCsvFile(File csvFile) {
		this.csvFile = csvFile
		def matcher = csvFile.name =~ namePattern
		if (matcher) {
			fileType = matcher[0][1]
			y2 = matcher[0][2]
		}
	}

}
