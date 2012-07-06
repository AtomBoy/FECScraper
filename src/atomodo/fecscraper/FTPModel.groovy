package atomodo.fecscraper

import org.apache.commons.net.ftp.FTPClient

class FTPModel {
	String server = "ftp.fec.gov"
	File localFolder 
	def filesToSkip = ["pas286.zip",]
	
	public FTPModel(File localFolder){
		this.localFolder = localFolder
		if (!localFolder.isDirectory() ) {
			localFolder.mkdirs()
		}
	}
	
	def update(){
		def ftp = new FTPClient()
		ftp.connect(server)
		ftp.login('anonymous', 'a@atomodo.com')
		println "Connected to $server. $ftp.replyString"
		ftp.changeWorkingDirectory('FEC')
		
		def ftpFiles = []

		['/FEC', '/FEC/2000', '/FEC/2002', '/FEC/2004', '/FEC/2006', '/FEC/2008'
			, '/FEC/2010', '/FEC/2012']. each {
			ftp.changeWorkingDirectory("${it}")
			ftpFiles.addAll(ftp.listFiles())
			ftpFiles = ftpFiles.findAll { it.name =~ FECDataFile.namePattern }
			ftpFiles.each { ftpFile ->
				String fileName = ftpFile.name
				File localFile = new File(localFolder, fileName)
				if (filesToSkip.contains(fileName)) {
					println "${fileName} is in list of files to skip."
				} else if (!localFile.exists()){
					localFile.withOutputStream{ os ->
						println "getting $fileName.."
						ftp.retrieveFile( fileName, os)
					}
				} else {
					if (localFile.length() != ftpFile.size) {
						println "${fileName} local file is ${localFile.length()} while remote file is ${ftpFile.size}"
						localFile.withOutputStream{ os ->
							println "getting $fileName.."
							ftp.retrieveFile( fileName, os)
						}
					} else {
						println "${fileName} is unchanged."
					}
				}
			}
		}		
		
		ftp.logout()
		ftp.disconnect()
		println "loged out.."
	}

}
