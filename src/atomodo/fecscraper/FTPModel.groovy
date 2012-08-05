package atomodo.fecscraper

import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTP

class FTPModel {
	static final int YEAR_FROM = 2004
	String server = "ftp.fec.gov"
	String logMessage = ""
	File localFolder 
	def filesToSkip = ["pas286.zip",]
	
	public FTPModel(File localFolder){
		this.localFolder = localFolder
		if (!localFolder.isDirectory() ) {
			localFolder.mkdirs()
		}
	}
	
	public String update(){
		def ftp = new FTPClient()
		ftp.connect(server)
		ftp.login('anonymous', 'a@atomodo.com')
		println "Connected to $server. $ftp.replyString"
		logMessage = "Connected to $server. $ftp.replyString\n"
		int fileCount = 0
		ftp.changeWorkingDirectory('FEC')
		ftp.setFileType(FTP.BINARY_FILE_TYPE)
		
		int yearNow = Integer.parseInt(new Date().format("yyyy"))
		for (int yyyy = YEAR_FROM; yyyy <= yearNow; yyyy += 2) {
			File yf = new File("${localFolder.getPath()}/FEC/${yyyy}")
			if (!yf.isDirectory()) {
				yf.mkdirs()
			}
			getFileNames(yyyy.toString()).each { fileName ->
				print fileName
				File localFile = 
					new File("${localFolder.getPath()}/FEC/${fileName}")
				def ftpFiles = ftp.listFiles("/FEC/${fileName}")
				if (ftpFiles) {
					def ftpFile = ftpFiles[0]
  					if (!localFile.isFile()) {
						print "..new file..getting.."
						localFile.withOutputStream{ os ->
							ftp.retrieveFile( fileName, os)
						}
						fileCount += 1
						print "done"
					} else if (localFile.length() != ftpFile.size) {
						print "..local=${localFile.length()} " +
							"remote=${ftpFile.size}..getting.."
							localFile.withOutputStream{ os ->
								ftp.retrieveFile( fileName, os)
							}
							fileCount += 1
							print "done"
					} else {
						print " ..unchanged"
					}
				} else {
					print "\t NOT FOUND!"
				}
				println "."
			}
			
		}
		ftp.logout()
		ftp.disconnect()
		println "loged out.."	
		logMessage += "GOT ${fileCount} new or updated files.\n"	
		return logMessage
	}
	
	static def getFileNames(String yyyy) {
		String yy = yyyy.toString().substring(2, 4)
		def fileNames = ["${yyyy}/cm${yy}.zip", "${yyyy}/cn${yy}.zip"
			, "${yyyy}/add${yy}.zip"
			, "${yyyy}/chg${yy}.zip", "${yyyy}/delete${yy}.zip"
			, "${yyyy}/indiv${yy}.zip", "${yyyy}/oth${yy}.zip"
			, "${yyyy}/pas2${yy}.zip"]
		return fileNames
	}
}
