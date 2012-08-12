package atomodo.fecscraper

import java.io.File;

class Main {

	static main(args) {
		File dataFolder = new File("./ftp.fec.gov")
		DBModel dbModel = new DBModel(dataFolder)
		dbModel.noteStartUpdate()
		FTPModel ftpModel = new FTPModel(dataFolder)
		dbModel.longMessage = ftpModel.update()
		dbModel.update()
	}

}
