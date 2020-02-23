import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import javax.net.ssl.HttpsURLConnection;

public class EquityDataDownload {

	static List<String> listHolidays = new ArrayList<String>();
	static List<String> listProcessed = new ArrayList<String>();
	static List<String> listDownloaded_dd = new ArrayList<String>();
	static List<String> listDownloaded_cm = new ArrayList<String>();
	static List<String> listDownloaded_fo = new ArrayList<String>();
	static List<String> listDownloaded_mto = new ArrayList<String>();

	static String HOME_PATH = "D:\\trade\\downloads";
//	static String UNZIP_PATH = "C:\\Users\\srshe";
	static String DD_FileLink = "https://www.nseindia.com/api/equity-stockIndices?csv=true&index=SECURITIES%20IN%20F%26O";
	static String CM_BaseLink = "https://www1.nseindia.com/content/historical/EQUITIES";
	static String FO_BaseLink = "https://www1.nseindia.com/content/historical/DERIVATIVES";
	static String MTO_BaseLink = "https://www1.nseindia.com/archives/equities/mto";
	static boolean CUSTOM_DATE_ENABLED = false;
	static String CUSTOM_DATE = "20-Feb-2020";

	public static void LoadHolidays() {
		listHolidays.add("21-Feb-2020");
		listHolidays.add("10-Mar-2020");
		listHolidays.add("02-Apr-2020");
		listHolidays.add("06-Apr-2020");
		listHolidays.add("10-Apr-2020");
		listHolidays.add("14-Apr-2020");
		listHolidays.add("01-May-2020");
		listHolidays.add("25-May-2020");
		listHolidays.add("02-Oct-2020");
		listHolidays.add("16-Nov-2020");
		listHolidays.add("30-Nov-2020");
		listHolidays.add("25-Dec-2020");
	}

	public static void LoadProcessed() {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(HOME_PATH + "\\" + "processed.txt"));
			String line = reader.readLine();
			while (line != null) {
				listProcessed.add(line);
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void LoadDownloaded() throws IOException {
		BufferedReader reader;

		reader = new BufferedReader(new FileReader(HOME_PATH + "\\" + "downloaded_dd.txt"));
		String line = reader.readLine();
		while (line != null) {
			listDownloaded_dd.add(line);
			// read next line
			line = reader.readLine();
		}
		reader.close();

		reader = new BufferedReader(new FileReader(HOME_PATH + "\\" + "downloaded_cm.txt"));
		line = reader.readLine();
		while (line != null) {
			listDownloaded_cm.add(line);
			// read next line
			line = reader.readLine();
		}
		reader.close();

		reader = new BufferedReader(new FileReader(HOME_PATH + "\\" + "downloaded_fo.txt"));
		line = reader.readLine();
		while (line != null) {
			listDownloaded_fo.add(line);
			// read next line
			line = reader.readLine();
		}
		reader.close();

		reader = new BufferedReader(new FileReader(HOME_PATH + "\\" + "downloaded_mto.txt"));
		line = reader.readLine();
		while (line != null) {
			listDownloaded_mto.add(line);
			// read next line
			line = reader.readLine();
		}
		reader.close();

	}

	public static boolean isProcessed(String sDate) {
		if (listProcessed.contains(sDate)) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isDownloaded(String sDate, String sType) {

		switch (sType) {
		case "dd":
			if (listDownloaded_dd.contains(sDate)) {
				return true;
			} else {
				return false;
			}
		case "cm":
			if (listDownloaded_cm.contains(sDate)) {
				return true;
			} else {
				return false;
			}

		case "fo":
			if (listDownloaded_fo.contains(sDate)) {
				return true;
			} else {
				return false;
			}

		case "mto":
			if (listDownloaded_mto.contains(sDate)) {
				return true;
			} else {
				return false;
			}
		default:
			break;
		}
		return false;
	}

	public static boolean isWeekend(String sDate) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");

		LocalDate localDate = LocalDate.parse(sDate, formatter);
		if ((localDate.getDayOfWeek() == DayOfWeek.SATURDAY || localDate.getDayOfWeek() == DayOfWeek.SUNDAY)) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isHoliday(String sDate) {
		if (listHolidays.contains(sDate)) {
			return true;
		} else {
			return false;
		}
	}

	public static String GetCmFileLink(String FileName) {
		// MW-SECURITIES-IN-F&O-20-Feb-2020
		final String regex = "([a-zA-Z&-]*)-([0-9][0-9])-([a-zA-Z][a-zA-Z][a-zA-Z])-([0-9][0-9][0-9][0-9]).csv";
		final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
		final Matcher matcher = pattern.matcher(FileName);

		// matcher.group(i)
		// Group 1: MW-SECURITIES-IN-F&O
		// Group 2: 21
		// Group 3: Feb
		// Group 4: 2020
		String cmFileLink = "";
		while (matcher.find()) {
			// https://www1.nseindia.com/content/historical/EQUITIES/2020/FEB/cm18FEB2020bhav.csv.zip
			cmFileLink = "https://www1.nseindia.com/content/historical/EQUITIES/" + matcher.group(4) + "/"
					+ matcher.group(3).toUpperCase() + "/" + "cm" + matcher.group(2) + matcher.group(3).toUpperCase()
					+ matcher.group(4) + "bhav.csv.zip";
		}
		return cmFileLink;
	}

	public static void downloadFile(String FileLink, String DestDir, String FileName, String FileType) {
		System.out.println(" Starting Download!!!!!!!!!!!");
		long startTime = System.currentTimeMillis();
		URL url = null;
		File fileFullPath = null;
		HttpsURLConnection httpsURLConnection = null;
		int i;
		try {

			url = new URL(FileLink);
			httpsURLConnection = (HttpsURLConnection) url.openConnection();
			httpsURLConnection.addRequestProperty("User-Agent",
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0");
			httpsURLConnection.addRequestProperty("Accept-Encoding", FileType);
			int responseCode = httpsURLConnection.getResponseCode();
			// always check HTTP response code first
			if (responseCode == httpsURLConnection.HTTP_OK) {
				System.out.println(" Opened connection ");
				if (FileName == null) {
					String fileName = "";
					String raw = httpsURLConnection.getHeaderField("Content-Disposition");
					// raw = "attachment; filename=abc.jpg"
					if (raw != null && raw.indexOf("=") != -1) {
						fileName = raw.split("=")[1]; // getting value after '='
					} else {
						fileName = "UnknownName.csv";
					}
					fileFullPath = new File(DestDir + "\\" + fileName);
				} else {
					fileFullPath = new File(DestDir + "\\" + FileName);
				}
				BufferedInputStream bufferedInputStream = new BufferedInputStream(httpsURLConnection.getInputStream());
				BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
						new FileOutputStream(fileFullPath));
				while ((i = bufferedInputStream.read()) != -1) {
					bufferedOutputStream.write(i);
				}
				bufferedOutputStream.flush();
				bufferedOutputStream.close();
				System.out.println(
						" Download Done !!!!!!!!!!!! | TimeTaken - " + (System.currentTimeMillis() - startTime));

			} else {
				System.out.println("No file to download. Server replied HTTP code: " + responseCode);

			}

		} catch (Exception exception) {
			System.err.println(" Exception occured while downloading " + exception);
			exception.printStackTrace();
			httpsURLConnection.disconnect();
		}
		httpsURLConnection.disconnect();
	}

	public static void updateFile(String FileName, String strDate) throws IOException {
		File file = new File(HOME_PATH + "\\" + FileName);
		FileWriter fr = new FileWriter(file, true);
		fr.write(strDate + "\n");
		fr.close();
	}

	public static void download_dd(String strDate) throws IOException {
		if (!listDownloaded_dd.contains(strDate)) {

			DateTimeFormatter formatter_s = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
			DateTimeFormatter formatter_d = DateTimeFormatter.ofPattern("ddMMyyyy");
			LocalDate localDate = LocalDate.parse(strDate, formatter_s);
			String strDate_d = formatter_d.format(localDate);
			String FileName = strDate_d + "_DD.csv";
			downloadFile(DD_FileLink, HOME_PATH + "\\" + "DD", FileName, "csv");
			updateFile("downloaded_dd.txt", strDate);
		} else {
			System.out.println("File " + strDate + " already downloaded");
		}
	}

	public static void download_cm(String strDate) throws IOException {
		// https://www1.nseindia.com/content/historical/EQUITIES/2020/FEB/cm20FEB2020bhav.csv.zip
		// https://www1.nseindia.com/content/historical/EQUITIES/2020/FEB/cm22FEB2020bhav.csv.zip
		// CM_BaseLink = https://www1.nseindia.com/content/historical/EQUITIES
		String fileName = "";
		String zipfileName = "";

		DateTimeFormatter formatter_s = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
		DateTimeFormatter formatter_d = DateTimeFormatter.ofPattern("ddMMyyyy");
		LocalDate localDate = LocalDate.parse(strDate, formatter_s);
		String strDate_d = formatter_d.format(localDate);

		if (!listDownloaded_cm.contains(strDate)) {

			final String regex = "([0-9][0-9])-([a-zA-Z][a-zA-Z][a-zA-Z])-([0-9][0-9][0-9][0-9])";
			final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
			final Matcher matcher = pattern.matcher(strDate);
			String sDate = "";
			String sMonth = "";
			String sYear = "";
			while (matcher.find()) {
				sDate = matcher.group(1);
				sMonth = matcher.group(2);
				sYear = matcher.group(3);
			}

			fileName = "cm" + sDate + sMonth.toUpperCase() + sYear + "bhav.csv";
			zipfileName = fileName + ".zip";
			String CM_DownloadLink = CM_BaseLink + "/" + sYear + "/" + sMonth.toUpperCase() + "/" + zipfileName;

			downloadFile(CM_DownloadLink, HOME_PATH + "\\" + "CM", zipfileName, "zip");

			String DestDir = HOME_PATH + "\\" + "CM";
			String ZipFile = HOME_PATH + "\\" + "CM\\" + zipfileName;
			unzip(ZipFile, DestDir);

			// Delete Zip file
			File file = new File(ZipFile);
			if (file.delete()) {
				System.out.println("Zip File deleted successfully");
			} else {
				System.out.println("Failed to delete the Zip file");
			}

			updateFile("downloaded_cm.txt", strDate);
		} else {
			System.out.println("File " + zipfileName + " already downloaded");
		}

		// Rename the downloaded file
		String newfileName = strDate_d + "_CM.csv";

		File oldfile = new File(HOME_PATH + "\\CM\\" + fileName);
		File newfile = new File(HOME_PATH + "\\CM\\" + newfileName);

		if (oldfile.renameTo(newfile)) {
			System.out.println("Rename succesful");
		} else {
			System.out.println("Rename failed");
		}

	}

	public static void download_fo(String strDate) throws IOException {
		// https://www1.nseindia.com/content/historical/DERIVATIVES/2020/FEB/fo20FEB2020bhav.csv.zip
		String fileName = "";
		String zipfileName = "";

		DateTimeFormatter formatter_s = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
		DateTimeFormatter formatter_d = DateTimeFormatter.ofPattern("ddMMyyyy");
		LocalDate localDate = LocalDate.parse(strDate, formatter_s);
		String strDate_d = formatter_d.format(localDate);

		if (!listDownloaded_fo.contains(strDate)) {

			final String regex = "([0-9][0-9])-([a-zA-Z][a-zA-Z][a-zA-Z])-([0-9][0-9][0-9][0-9])";
			final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
			final Matcher matcher = pattern.matcher(strDate);
			String sDate = "";
			String sMonth = "";
			String sYear = "";
			while (matcher.find()) {
				sDate = matcher.group(1);
				sMonth = matcher.group(2);
				sYear = matcher.group(3);
			}

			fileName = "fo" + sDate + sMonth.toUpperCase() + sYear + "bhav.csv";
			zipfileName = fileName + ".zip";
			String FO_DownloadLink = FO_BaseLink + "/" + sYear + "/" + sMonth.toUpperCase() + "/" + zipfileName;

			downloadFile(FO_DownloadLink, HOME_PATH + "\\" + "FO", zipfileName, "zip");

			String DestDir = HOME_PATH + "\\" + "FO";
			String ZipFile = HOME_PATH + "\\" + "FO\\" + zipfileName;
			unzip(ZipFile, DestDir);

			// Delete Zip file
			File file = new File(ZipFile);
			if (file.delete()) {
				System.out.println("Zip File deleted successfully");
			} else {
				System.out.println("Failed to delete the Zip file");
			}

			updateFile("downloaded_fo.txt", strDate);
		} else {
			System.out.println("File " + zipfileName + " already downloaded");
		}

		// Rename the downloaded file
		String newfileName = strDate_d + "_FO.csv";

		File oldfile = new File(HOME_PATH + "\\FO\\" + fileName);
		File newfile = new File(HOME_PATH + "\\FO\\" + newfileName);

		if (oldfile.renameTo(newfile)) {
			System.out.println("Rename succesful");
		} else {
			System.out.println("Rename failed");
		}

	}

	public static void download_mto(String strDate) throws IOException {
		// https://www1.nseindia.com/archives/equities/mto/MTO_20022020.DAT
		String fileName = "";

		DateTimeFormatter formatter_s = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
		DateTimeFormatter formatter_d = DateTimeFormatter.ofPattern("ddMMyyyy");
		LocalDate localDate = LocalDate.parse(strDate, formatter_s);
		String strDate_d = formatter_d.format(localDate);

		if (!listDownloaded_mto.contains(strDate)) {

			fileName = "MTO_" + strDate_d + ".DAT";
			String MTO_DownloadLink = MTO_BaseLink + "/" + fileName;

			downloadFile(MTO_DownloadLink, HOME_PATH + "\\" + "MTO", fileName, "ASCII");

			updateFile("downloaded_mto.txt", strDate);
		} else {
			System.out.println("File " + fileName + " already downloaded");
		}

		// Rename the downloaded file
		String newfileName = strDate_d + "_MTO.DAT";

		File oldfile = new File(HOME_PATH + "\\MTO\\" + fileName);
		File newfile = new File(HOME_PATH + "\\MTO\\" + newfileName);

		if (oldfile.renameTo(newfile)) {
			System.out.println("Rename succesful");
		} else {
			System.out.println("Rename failed");
		}

	}

	public static void loadProperties() throws Exception
	{
		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream(".\\configuration.xml");
		//FileInputStream fist = EquityDataDownload.class.getResourceAsStream("./configuration.xml");
//		InputStream  fis = EquityDataDownload.class.getResourceAsStream("D:\\trade\\downloads\\configuration.xml");
		
		properties.loadFromXML(fis);
		
		if (properties.getProperty("HOME_PATH") != null)
		{
			HOME_PATH = properties.getProperty("HOME_PATH");
		}
		if (properties.getProperty("DD_FileLink") != null)
		{
			DD_FileLink = properties.getProperty("DD_FileLink");
		}
		if (properties.getProperty("CM_BaseLink") != null)
		{
			CM_BaseLink = properties.getProperty("CM_BaseLink");
		}
		if (properties.getProperty("FO_BaseLink") != null)
		{
			FO_BaseLink = properties.getProperty("FO_BaseLink");
		}
		if (properties.getProperty("MTO_BaseLink") != null)
		{
			MTO_BaseLink = properties.getProperty("MTO_BaseLink");
		}
		if (properties.getProperty("CUSTOM_DATE_ENABLED") != null)
		{
			CUSTOM_DATE_ENABLED = Boolean.parseBoolean(properties.getProperty("CUSTOM_DATE_ENABLED"));
		}
		if (properties.getProperty("CUSTOM_DATE") != null)
		{
			CUSTOM_DATE = properties.getProperty("CUSTOM_DATE");
		}
		
		System.out.println(CUSTOM_DATE);
	}
	
	public static void main(String[] args) throws Exception {
		loadProperties();
		LoadHolidays();
		LoadProcessed();
		LoadDownloaded();

//Get current date
		Date date = new Date();
		DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		String strDate = dateFormat.format(date);
		if (CUSTOM_DATE_ENABLED) {
			strDate = CUSTOM_DATE;
		}

//download_dd("20-Feb-2020");
//download_cm("20-Feb-2020");
//download_fo("20-Feb-2020");
//download_mto("20-Feb-2020");
		if (!isWeekend(strDate) && !isHoliday(strDate)) {
			if (!isProcessed(strDate)) {

				if (!isDownloaded(strDate, "dd")) {
					download_dd(strDate);
				}

				if (!isDownloaded(strDate, "cm")) {
					download_cm(strDate);
				}
				if (!isDownloaded(strDate, "fo")) {
					download_fo(strDate);
				}

				if (!isDownloaded(strDate, "mto")) {
					download_mto(strDate);
				}
			} else {
				System.out.println("Processed for the current date already.");
			}
		} else {
			System.out.println("Weekend or Holiday");
		}

	}

	public static void unzip(String zipFilePath, String destDir) {
		File dir = new File(destDir);
		// create output directory if it doesn't exist
		if (!dir.exists())
			dir.mkdirs();
		FileInputStream fis;
		// buffer for read and write data to file
		byte[] buffer = new byte[1024];
		try {
			fis = new FileInputStream(zipFilePath);
			ZipInputStream zis = new ZipInputStream(fis);
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String fileName = ze.getName();
				File newFile = new File(destDir + File.separator + fileName);
				System.out.println("Unzipping to " + newFile.getAbsolutePath());
				// create directories for sub directories in zip
				new File(newFile.getParent()).mkdirs();
				FileOutputStream fos = new FileOutputStream(newFile);
				int len;
				while ((len = zis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}
				fos.close();
				// close this ZipEntry
				zis.closeEntry();
				ze = zis.getNextEntry();
			}
			// close last ZipEntry
			zis.closeEntry();
			zis.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}