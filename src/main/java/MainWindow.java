import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;
import net.miginfocom.swing.MigLayout;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import javax.swing.*;
import javax.swing.filechooser.*;
import javax.swing.filechooser.FileFilter;
import java.awt.Cursor;
import java.awt.datatransfer.DataFlavor;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * 03/02/17 frahman Created with IntelliJ IDEA.
 */
public class MainWindow {
    private static Logger LOG = Logger.getLogger(MainWindow.class);

    private static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
    private static final String DBCONN_TEST = "jdbc:jtds:sqlserver://RAT-SQL-APPS-TEST.rat-allianz.com:1433/master;maxStatements=0";
    private static final String DBCONN_PROD = "jdbc:jtds:sqlserver://RAT-SQL-RMS.rat-allianz.com:1433/master;maxStatements=0";
    private static final String AUTH_CONN_SUFFIX = ";useNTLMv2=tru‌​e;domain=RAT-ALLIANZ";

    private final EdmRdmFilterWithBaks edmRdmFilterWithBaks = new EdmRdmFilterWithBaks();
    private final EdmRdmFilterNoBaks edmRdmFilterNoBaks = new EdmRdmFilterNoBaks();

    private String getUniversalSqlServerTempBakFileDir(String destSmbUrl) {

        String dir = null;
        if( destSmbUrl.contains("RAT-SQL-RMS") ||
            destSmbUrl.contains(getSystemProperty("PROD_IP_ADDRESS", Data.PROD_IP_ADDRESS_DEFAULT) )){
            log("PRODUCTION RMS DB matches '"+destSmbUrl+"'");
            dir = getSystemProperty("UniversalTempBakDirProd", Data.PROD_SQL_SERVER_TEMP_BAK_DIR_SMB);
        }else if(destSmbUrl.contains("RAT-SQL-APPS-TEST") ||
            destSmbUrl.contains(getSystemProperty("TEST_IP_ADDRESS", Data.TEST_IP_ADDRESS_DEFAULT) )){
            log("APPS TEST DB matches '"+destSmbUrl+"'");
            dir = getSystemProperty("UniversalTempBakDirTest", Data.TEST_SQL_SERVER_TEMP_BAK_DIR_SMB);
        }else if(destSmbUrl.contains( getSystemProperty("MYPC_HOSTNAME", Data.MYPC_HOSTNAME_DEFAULT) ) ||
            destSmbUrl.contains( getSystemProperty("MYPC_IP_ADDRESS", Data.MYPC_IP_ADDRESS_DEFAULT) )){
            log("Local temp/dest folder matches '"+destSmbUrl+"'");
            dir = "smb://"+System.getProperty("MYPC_IP_ADDRESS_DEFAULT", "10.200.205.164")+"/C$/temp/dest/";
        } else {
            String err = "ERROR: Unrecognised DB Server'"+destSmbUrl+"'";
            log(err);
            throw new IllegalArgumentException(err);
        }

        return dir;
    }

    private static File getLocalSqlServerMdfOrLdfFileDir(final String filename, String destName) {
        if (FilenameUtils.isExtension(filename.toUpperCase(), "MDF")) {
            return getLocalSqlServerMdfFileDir(destName);
        } else if (FilenameUtils.isExtension(filename.toUpperCase(), "LDF")) {
            return getLocalSqlServerLdfFileDir(destName);
        } else {
            throw new RuntimeException("Expected an MDF or LDF file, got: '" + filename + "'");
        }
    }

    private GUIActions guiActions = new GUIActions();   // http://stackoverflow.com/a/5451187/7409029

    private JCheckBox chkBatchMode;

    private JScrollPane batchPane;
    private JList batchList;
    private DefaultListModel<String> batchModel;
    private JButton deleteButton;

    private JButton fileFolderPickerButton;
    private JTextField txtUserName;
    private JPasswordField txtPassword;
    private JTextField txtInputPathName;
    private JPanel mainPanel;
    private Cursor normalCursor;
    private final Cursor waitCursor = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR);
    private JComboBox destsCombo;
    private JTextPane logWindow;
    private JProgressBar progressBar;
    private JButton installDatabaseButton;

    private Map<String, String> uncCopyDests = new HashMap<>();
    private Map<String, String> smbCopyDests = new HashMap<>();

    //Wean off the IDEA layout builder so we can run this outside the IDE
    //
    private void createLayoutGUI(){
        mainPanel = new JPanel( new MigLayout());

        chkBatchMode = new JCheckBox("Batch mode");
        chkBatchMode.setToolTipText("Allows a few ways to build a batch of mdfs to run unattended");
        mainPanel.add(chkBatchMode, "span 3, grow, wrap" );

        batchModel = new DefaultListModel<String>();
        batchList = new JList(batchModel);
        batchList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        batchList.setLayoutOrientation(JList.VERTICAL);
        batchList.setVisibleRowCount(7);
        batchList.setToolTipText("In batch mode, selected files will be added here (*.MDF only)");

        batchPane = new JScrollPane(batchList);
        mainPanel.add( batchPane, "span 5 3, grow, wrap" );

        fileFolderPickerButton = new JButton("Choose file");
        fileFolderPickerButton.setToolTipText("Select the RMS database to install");
        mainPanel.add(fileFolderPickerButton, "span 1, grow");

        installDatabaseButton = new JButton("Install");
        installDatabaseButton.setEnabled(false);
        installDatabaseButton.setToolTipText("Processes the selected file or batch of files");
        mainPanel.add(installDatabaseButton, "span 1, grow");

        destsCombo = new JComboBox();
        destsCombo.setToolTipText("Only for advanced use");
        mainPanel.add(destsCombo, "span 1, grow, wrap");
        //--------------------------------------------------

        txtInputPathName = new JTextField();
        txtInputPathName.setToolTipText("Tip: To brew a quick batch, paste a CSV list of pathnames here..");
        mainPanel.add(txtInputPathName ,   "span, growx, wrap");
        //--------------------------------------------------

        deleteButton = new JButton("Remove selected(s)");
        deleteButton.setToolTipText("Remove selected items from batch");
        mainPanel.add(deleteButton, "span 2, grow, wrap");
        //--------------------------------------------------

        mainPanel.add(new JLabel("User:"));
        txtUserName = new JTextField("poweruser-admin");
        txtUserName.setToolTipText("Enter Windows user name");
        mainPanel.add(txtUserName, "wrap");
        //--------------------------------------------------

        mainPanel.add(new JLabel("Pass:"));
        txtPassword = new JPasswordField("slartibartfarst");
        txtPassword.setToolTipText("Psssst... can you keep a secret?");
        mainPanel.add(txtPassword, "wrap");
        //--------------------------------------------------

        progressBar = new JProgressBar();
        progressBar.setToolTipText("Watch progress here");
        mainPanel.add(progressBar, "span, grow, wrap");
        //--------------------------------------------------

        logWindow = new JTextPane();
        JScrollPane logScroller = new JScrollPane(logWindow);
        logWindow.setText("Log output will be added here.\n");
        mainPanel.add(logScroller, "span, grow");

        // Even easier to build a batch of mdfs by dragging from windows exploder.
        //
        // Kindly shared by RustyX over at http://stackoverflow.com/a/9111327/7409029
        //
        mainPanel.setDropTarget(new DropTarget() {
            public synchronized void drop(DropTargetDropEvent evt) {
                try {
                    evt.acceptDrop(DnDConstants.ACTION_COPY);
                    List<File> droppedFiles = (List<File>)
                        evt.getTransferable().getTransferData(DataFlavor.javaFileListFlavor);

                    // if multiple files dropped, switch to batch mode
                    if(droppedFiles.size() > 1 || chkBatchMode.isSelected()){
                        offerFilesToBatch(droppedFiles);
                    }else{
                        //accept single file
                        File file = droppedFiles.get(0);
                        String dropped = file.getAbsolutePath();
                        if(edmRdmFilterWithBaks.accept(file)){
                            log("Dropped in: "+ dropped);
                            txtInputPathName.setText(dropped);
                        }else{
                            log("Reject drop:"+ dropped);
                        }
                    }
                    checkEnableButtons();
                } catch (Exception ex) {
                    popTitledErrorDialog("Drag drop exception", ex.getMessage());
                    ex.printStackTrace();
                }
            }
        });
    }

    // Idiot proofing against 'click first and ask questions later' brigade...
    //
    private void lockGui(){
        batchList.setEnabled(false);
        deleteButton.setEnabled(false);
        chkBatchMode.setEnabled(false);
        fileFolderPickerButton.setEnabled(false);
        txtInputPathName.setEnabled(false);
        destsCombo.setEnabled(false);
        txtUserName.setEnabled(false);
        txtPassword.setEnabled(false);
        installDatabaseButton.setEnabled(false);
        mainPanel.setCursor(waitCursor);
    }
    private void unlockGui(){
        batchList.setEnabled(true);
        deleteButton.setEnabled(true);
        chkBatchMode.setEnabled(true);
        fileFolderPickerButton.setEnabled(true);
        txtInputPathName.setEnabled(true);
        destsCombo.setEnabled(true);
        txtUserName.setEnabled(true);
        txtPassword.setEnabled(true);
        installDatabaseButton.setEnabled(true);
        mainPanel.setCursor(normalCursor);
    }

    private void popTitledErrorDialog(String title, String message){
        JOptionPane.showMessageDialog(mainPanel, message, title, JOptionPane.ERROR_MESSAGE);
    }

    private void popErrorDialog(String message){
        JOptionPane.showMessageDialog(mainPanel, message, "Oops!", JOptionPane.ERROR_MESSAGE);
    }

    private void offerFileToBatch(File file, DefaultListModel<String> batchModel){
        offerFileToBatch(file.getAbsolutePath(), batchModel);
    }

    private void offerFileToBatch(String path, DefaultListModel<String> batchModel){
        if(!batchModel.contains(path)){
            if(containsFileOfDifferentType(FilenameUtils.getExtension(path), batchModel.elements())){
                popTitledErrorDialog("Mixed batches not supported", "Rejecting: "+path);
                log("Rejecting: " + path);
            }else{
                batchModel.addElement(path);
                log("Add batch: " + path);
            }
        }else{
            popTitledErrorDialog("Duplicate file rejected ", path);
        }
    }

    void offerFilesToBatch(List<File> candidateFiles){
        if(!chkBatchMode.isSelected()){
            chkBatchMode.setSelected(true);
            txtInputPathName.setEnabled(false);
            batchList.setEnabled(true);
        }
        int origSize = batchModel.size();
        for (File file : candidateFiles) {
            //accept batch-compatible files only
            if(edmRdmFilterNoBaks.accept(file)){
                offerFileToBatch(file, batchModel);
            }else{
                log("Batch rejects: "+file.getAbsolutePath());
            }
        }
        if(batchModel.size()>origSize){
            batchList.setSelectionInterval(origSize,batchModel.size()-1);
        }
    }

    private void popConfirmDialog(String message){
        JOptionPane.showMessageDialog(mainPanel, message, "Information", JOptionPane.INFORMATION_MESSAGE);
    }

    class EdmRdmFilterWithBaks extends FileFilter {

        public String getDescription(){
            return "EDM or RDM databases (ending in MDF or BAK)";
        }

        public boolean accept( File f ) {
            if( f == null ){
                return false;
            }
            if (f.isDirectory()) {
                return true;
            }

            String extension = FilenameUtils.getExtension(f.getName());

            if( extension.equalsIgnoreCase("mdf") ||
                extension.equalsIgnoreCase("bak")    ){

                String baseName = FilenameUtils.getBaseName(f.getName());

                if( StringUtils.containsIgnoreCase(baseName,"EDM") ||
                    StringUtils.containsIgnoreCase(baseName,"RDM")    ){

                    return true;
                }
            }

            return false;
        }
    }
    // Temporary - till batches support BAK files
    class EdmRdmFilterNoBaks extends FileFilter {

        public String getDescription(){
            return "EDM or RDM databases (only *.MDF for batches)";
        }

        public boolean accept( File f ) {
            if( f == null ){
                return false;
            }
            if (f.isDirectory()) {
                return true;
            }

            if( FilenameUtils.getExtension(f.getName()).equalsIgnoreCase("mdf") ){
                String baseName = FilenameUtils.getBaseName(f.getName());
                if( StringUtils.containsIgnoreCase(baseName,"EDM") ||
                    StringUtils.containsIgnoreCase(baseName,"RDM")    ){
                    return true;
                }
            }
            return false;
        }
    }
    // This nice idiom suggested in http://stackoverflow.com/a/5451187/7409029
    //
    class GUIActions {

        // runs on background thread
        void restoreOneMdf( final ActionEvent e, String inputPathname, String destName) throws Exception{
            //first copy to server
            if(!destName.startsWith("Local")){
                secureCopyFile(
                    inputPathname,
                    smbCopyDests.get(destName),
                    txtUserName.getText(),
                    txtPassword.getPassword(),
                    progressBar
                );
            }else{
                copyFile(inputPathname, uncCopyDests.get(destName), progressBar);
            }

            //then attach db
            attachDBAction(e,uncCopyDests.get(destName), inputPathname);

            //finally add user rights and map to rms version
            mapDatabaseAction(e,uncCopyDests.get(destName), inputPathname);

            if(Mailer.sendMailConfirms){
                Mailer.sendMail("Just attached RMS: "+inputPathname, "Please talk to Mr Bullock if you have any questions"+ "\n***\n"+logWindow.getText());
            }

        }

        // Secure copy, attach and map in one click
        // If batch mode, process each mdf found in batch.
        public void restoreMDFAction(final ActionEvent e, final String destName){
            log("Action: " + e.getActionCommand() );

            try{
                Thread thread = new Thread(
                    new Runnable(){
                        public void run(){
                            // Use a try block to allow failing early steps to skip later steps
                            int batchEntriesDone = 0;
                            try{
                                if( chkBatchMode.isSelected() ){
                                    for(Object srcPathname : batchModel.toArray()){
                                        if( FilenameUtils.getExtension(""+srcPathname).equalsIgnoreCase("mdf") ){
                                            restoreOneMdf(e, ""+srcPathname, destName);
                                            ++batchEntriesDone;
                                            log("Restored " + srcPathname + " to " + destName);
                                            batchModel.removeElement(srcPathname);
                                        }
                                    }
                                    popConfirmDialog(batchEntriesDone + " MDFs restored OK");
                                }else{
                                    restoreOneMdf(e, txtInputPathName.getText(), destName);
                                    popConfirmDialog("MDF restored OK");
                                }

                            } catch( Exception t ){ // Don't catch Throwable - maybe used for system purposes ?
                                String err = "FAILED restore to '" + destsCombo.getSelectedItem()+"'";
                                if( batchEntriesDone>0 ){
                                    err += "\n..after " + batchEntriesDone + " succeeded.";
                                }
                                log(err, t);
                                Mailer.sendMail("Failure..", err + "\n***\n"+logWindow.getText());
                                popErrorDialog(err + "\n" + t.getMessage());
                                unlockGui();
                            }
                        }
                    }
                );
                thread.start();
            }catch( Throwable throwable ){
                String err = "ERROR processing MDF:'" + txtInputPathName.getText() + "'";
                log(err, throwable);
                popErrorDialog(err + "\n" + throwable.getMessage());
            }
        }

        // TODO probably could peel off one layer of try-catch here
        //
        String attachDBAction(ActionEvent e, String uncDest, String inputPathname){
            log(String.format("Action: '%s', dest: '%s'", e.getActionCommand(), uncDest ) );
            try{
                final String mdfFileName = new File(inputPathname).getName();
                final String ext = FilenameUtils.getExtension(mdfFileName);
                assert !StringUtils.isBlank(ext) &&  ext.equalsIgnoreCase("MDF");

                final String dbName = FilenameUtils.getBaseName(mdfFileName);
                final String localPath = new File(getLocalSqlServerMdfFileDir(destsCombo.getSelectedItem().toString()), mdfFileName).getPath();
                final String sql = "CREATE DATABASE [" + dbName + "] ON (FILENAME = '" + localPath + "') FOR ATTACH;";
                final Connection conn = connectDB(uncDest);
                log("Connected to DB for dest: " + uncDest);
                if( conn != null ){
                    progressBar.setString("**Attaching DB on Server**");
                    progressBar.setValue(0);
                    lockGui();
                    try (final Statement statement = conn.createStatement()) {
                        statement.execute(sql);
                        log("OK: Attached using sql: '" + sql + "'");

                        progressBar.setString("Attached " + dbName);
                        progressBar.setValue(100);
                    }
                    catch( SQLException sqx){
                        progressBar.setString("!!!Failed attaching " + dbName+"!!!");
                        String msg ="FAILED attach DB ("+sql+")";
                        log(msg, sqx);
                        throw  new IllegalStateException(msg + "\n" + sqx.getMessage());
                    } finally {
                        unlockGui();
                    }
                    return dbName;
                } else {
                    throw new IllegalArgumentException("Failed connect DB");
                }
            }catch(Exception ex){
                progressBar.setString("!!!Error attaching DB!!!");
                log("Error",ex);
//                popErrorDialog(ex.getMessage());  // causes extra error dialog that delays email
                throw ex;
            }
        }

        // Connect using creds if supplied, else without
        // Try to match selected destination to either PROD RMS or APPS TEST dbs.
        //
        private Connection connectDB(String destUrl) {
            String connectionString = null;
            if( destUrl.contains("RAT-SQL-RMS") ||
                destUrl.contains(getSystemProperty("PROD_IP_ADDRESS", Data.PROD_IP_ADDRESS_DEFAULT) )){
                log("PRODUCTION RMS DB matches '"+destUrl+"'");
                connectionString = DBCONN_PROD;
            }else if(destUrl.contains("RAT-SQL-APPS-TEST") ||
                    destUrl.contains(getSystemProperty("TEST_IP_ADDRESS", Data.TEST_IP_ADDRESS_DEFAULT) )){
                log("APPS TEST DB matches '"+destUrl+"'");
                connectionString = DBCONN_TEST;
            }else if(destUrl.contains("-RAT") ||
                destUrl.contains(getSystemProperty("MYPC_IP_ADDRESS", Data.MYPC_IP_ADDRESS_DEFAULT) )){
                log("Local PC matches '"+destUrl+"'");
                connectionString = "jdbc:jtds:sqlserver://localhost:1433/master;instance=SQLEXPRESS;integratedSecurity=true;";
            } else {
                log("ERROR: Unrecognised DB Server'"+destUrl+"'");
            }

            try{
                if( connectionString == null ) {
                    throw new IllegalArgumentException("UNKNOWN DB destination: "+destUrl);
                }
                loadJDBCDriver(JDBC_DRIVER);
                if( (connectionString == DBCONN_TEST || connectionString == DBCONN_PROD) &&
                    StringUtils.isNotBlank(txtUserName.getText()) &&
                    StringUtils.isNotBlank(String.valueOf(txtPassword.getPassword()))){
                    connectionString += AUTH_CONN_SUFFIX;
                    log("Getting auth conn: " + connectionString);
                    return DriverManager.getConnection(
                        connectionString,
                        txtUserName.getText().trim(),
                        String.valueOf(txtPassword.getPassword())
                    );
                }else{
                    log("Getting simple (no credentials) conn: " + connectionString);
                    return DriverManager.getConnection( connectionString );
                }
            }catch(ClassNotFoundException cnf){
                log("FAILED to load JDBC driver: "+JDBC_DRIVER, cnf);
                throw new IllegalArgumentException("Failed to connect to DB (JDBC driver missing?)");
            }catch(SQLException sqx){
                log("FAILED to connect with: "+connectionString, sqx);
                throw new IllegalArgumentException("Failed to connect to DB\n"+sqx.getMessage());
            }
        }

        // Simple copy: use unc paths
        public void simpleCopyAction(ActionEvent e){
            log("Action: " + e.getActionCommand());
            try{
                final String uncDest =uncCopyDests.get(destsCombo.getSelectedItem());
                Thread thread = new Thread(
                    new Runnable(){
                        public void run(){
                            try{
                                copyFile(txtInputPathName.getText(), uncDest, progressBar);
                            }catch(Exception ex){

                            }
                        }
                    }
                );
                thread.start();
            }catch( Throwable ioe ){
                progressBar.setString("!!!Copy failed!!!");
                String err = "Failed to copy '" + txtInputPathName.getText() + "' to '" + destsCombo.getSelectedItem()+"'";
                log(err, ioe);
                popErrorDialog(err+"\n"+ioe.getMessage());
            }
        }

        // When batch mode is selected, disable the input pathname textfield
        // Also move any pathname(s) already there into the batch list
        //
        public void batchModeAction(ActionEvent e){
            log("Action: " + e.getActionCommand() + (chkBatchMode.isSelected()?" checked":" cleared") );

            txtInputPathName.setEnabled(!chkBatchMode.isSelected());
            batchList.setEnabled(chkBatchMode.isSelected());

            // Import from the text field as a useful shortcut to selecting a batch
            //
            if( chkBatchMode.isSelected() ){
                if( StringUtils.isNotBlank(txtInputPathName.getText()) ){
                    List<String> pathList = Arrays.asList(
                        txtInputPathName.getText().trim().split("[,|;]")
                    );
                    pathList.removeAll(Arrays.asList("", null));
                    offerFilesToBatch(
                        Lists.transform(pathList, new Function<String,File>() {
                            public File apply(String path) {
                                return new File(path);
                            }
                    }));

                    txtInputPathName.setText("");
                    checkEnableButtons();
                }
            }
        }

        public void chooseSourceAction(ActionEvent e){
            log("Action: " + e.getActionCommand() + ", mods:" + e.getModifiers() );
            JFileChooser fc = new JFileChooser("C:\\temp");

            fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
            fc.setMultiSelectionEnabled(true);

            // Add convenience filters
            fc.setFileFilter(new FileNameExtensionFilter("BAK files", "bak"));
            fc.setFileFilter(new FileNameExtensionFilter("MDF files", "mdf"));
            fc.setFileFilter(chkBatchMode.isSelected() ? edmRdmFilterNoBaks : edmRdmFilterWithBaks);
            if( fc.showOpenDialog(mainPanel) == JFileChooser.APPROVE_OPTION ){
                File[] files = fc.getSelectedFiles();

                if(files.length > 1 || chkBatchMode.isSelected()){
                    offerFilesToBatch(Arrays.asList(files));
                } else if(files.length == 1){
                    txtInputPathName.setText( files[0].getAbsolutePath() );
                    log( "Selected file: "+ files[0].getAbsolutePath() );
                }
                checkEnableButtons();
            }
        }

        private String getLogicalName(Map<String,String> dict){

            String name = null;

            if(!dict.isEmpty()){
                for( String key : dict.keySet() ){
                    if( !key.contains("_log") ){
                        name = key;
                    }
                }
                if(name != null){
                    return name;
                }
                throw new IllegalArgumentException("No logical name found in keyset: "+dict.keySet().toArray());
            }

            throw new IllegalArgumentException("Got empty map of logical to physical names!");
        }

        private Map<String,String> extractLogicalPhysicalNames( Connection conn, File localTempBakFile, String destName){
            final Map<String, String> logicalNameToPathname = new HashMap<>();
            final String sqlListFiles = String.format(
                "restore filelistonly from disk = '%s'", localTempBakFile
            );
            try (final PreparedStatement statement = conn.prepareStatement(sqlListFiles)) {
                final ResultSet resultSet = statement.executeQuery();
                if(resultSet.next()) {
                    do {
                        final String logicalName = resultSet.getString("LogicalName");
                        final String physicalName = resultSet.getString("PhysicalName");

                        final File physicalLocalDirOnSqlServer = getLocalSqlServerMdfOrLdfFileDir(physicalName, destName);
                        final String physicalPathname = new File(physicalLocalDirOnSqlServer, new File(physicalName).getName()).getAbsolutePath();
                        log(String.format("Mapping logical to physical [%s] -> [%s]",logicalName,physicalPathname));
                        logicalNameToPathname.put(logicalName, physicalPathname);
                    } while (resultSet.next());

                    return logicalNameToPathname;

                } else {
                    throw new IllegalStateException("No filelist in BAK file using SQL [" + sqlListFiles + "]");
                }
            }catch( SQLException sqx ){
                String msg ="FAILED to list files via ["+sqlListFiles+"]";
                log(msg, sqx);
                throw  new IllegalStateException(msg+"\n"+sqx.getMessage(),sqx);
            }
        }

        public void restoreBAKAction(ActionEvent e, final String destName){
            log(String.format("Action: '%s', destName: '%s'", e.getActionCommand(), destName ) );

            try{
                Thread thread = new Thread( new Runnable(){
                    public void run(){
                        try{
                            //first copy to server - if this throws we skip pointless later steps
                            //
                            if(!destName.startsWith("Local")){
                                secureCopyFile(
                                    txtInputPathName.getText(),
                                    getUniversalSqlServerTempBakFileDir(smbCopyDests.get(destName)),
                                    txtUserName.getText(),
                                    txtPassword.getPassword(),
                                    progressBar
                                );
                            }else{
                                copyFile(txtInputPathName.getText(),"C:/temp/dest", progressBar);
                            }

                            // Extract logical / physical names from backup file
                            //
                            final Connection conn = connectDB(smbCopyDests.get(destName));
                            if( conn != null ){
                                log("Connected to DB for destName: " + destName);
                                final File localTempBakFile = new File(
                                    (    destName.startsWith("Local")
                                    ?    "C:/temp/dest"
                                    :    getSystemProperty("LocalTempBakDirPath", Data.DEFAULT_LOCAL_SQL_SERVER_TEMP_BAK_DIR)
                                    ),
                                    FilenameUtils.getName(txtInputPathName.getText())
                                );
                                progressBar.setString("***Identifying DB name..***");
                                final Map<String, String> logicalNameToPathname =  extractLogicalPhysicalNames(conn,localTempBakFile, destName);

                                // Basename is not reliably the DB name so try  logical name first
                                //
                                String dbName = null;
                                try{
                                    dbName = getLogicalName(logicalNameToPathname);
                                }catch(IllegalArgumentException iae){
                                    dbName = FilenameUtils.getBaseName(txtInputPathName.getText());
                                    log("Failed to get dbname from logical name; fallback to bakfile basename:"+dbName);
                                }

                                // Check the DB doesn't already exist!!
                                verifyDbNotAlreadyExists(dbName,conn);

                                try (final Statement statement = conn.createStatement()) {
                                    String sqlRestoreDB = "restore database [" + dbName + "] \n"
                                        + "from disk = '" + localTempBakFile.getAbsolutePath() + "' \n"
                                        + "WITH \n"
                                        + Joiner.on(",\n").join(Iterables.transform(logicalNameToPathname.entrySet(), new Function<Map.Entry<String, String>, String>() {
                                        @Override
                                        public String apply(final Map.Entry<String, String> logicalFileInBakToNewPhysFile) {
                                            return "  MOVE '" + logicalFileInBakToNewPhysFile.getKey() + "' TO '" + logicalFileInBakToNewPhysFile.getValue() + "'";
                                        }
                                    }));
                                    log("Restoring database using SQL:\n" + sqlRestoreDB);

                                    progressBar.setString("***Restoring " + dbName + "..***");
                                    lockGui();
                                    statement.executeUpdate(sqlRestoreDB);
                                    unlockGui();
                                    progressBar.setString("Restored " + dbName );
                                    String confirm = String.format("Restored DB '%s' on '%s'\n\n(Will next setup user rights and Artisan mappings to RMS versions..)", dbName, destName);
                                    log(confirm);
                                }
                                progressBar.setString("***Adding rights to " + dbName + "..***");
                                lockGui();
                                setupRightsNewStyle(dbName, conn);
                                if(Mailer.sendMailConfirms){
                                    Mailer.sendMail("Just restored RMS backup: " + dbName, "Please talk to Mr Bullock if you have any questions"+ "\n***\n"+logWindow.getText());
                                }
                                unlockGui();
                                progressBar.setString("Installed DB");
                                log("OK: Added user rights (and rms version mappings) for DB '"+dbName+"'");
                                popConfirmDialog("Backup restored OK");

                            } else { // null conn
                                log("FAILED to connect to DB for destName: " + destName);
                                throw new IllegalArgumentException("FAILED to connect to DB for destName: " + destName);
                            }

                        } catch( Exception t ){ // Don't catch Throwable - maybe used for system purposes ?
                            String err = "Error restoring: '" + txtInputPathName.getText() + "' onto '" + destsCombo.getSelectedItem()+"'";
                            log(err, t);
                            Mailer.sendMail(err, err + "\n***\n"+logWindow.getText());
                            popErrorDialog(err + "\n" + t.getMessage());
                            unlockGui();
                        }
                    }
                });
                thread.start();
            }catch( Throwable throwable ){
                String err = "ERROR processing BAK:'" + txtInputPathName.getText() + "'";
                log(err, throwable);
                popErrorDialog(err + "\n" + throwable.getMessage());
            }

        }

        // Usecase - restoring a BAK file, we've copied the DB file and about to restore the DB from it.
        // Before we restore it to compatible RMS versions, check it's not going to be overwritten
        //
        private void verifyDbNotAlreadyExists(String dbName, Connection conn){
            log(String.format("Doing verifyDbNotAlreadyExists, dbName=%s", dbName));
            final String sql = String.format(
                "select count(*) from sys.databases where name = '%s'",
                dbName
            );
            final String failed ="FAILED to query db existence: ["+sql+"]";
            try (final Statement statement = conn.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                if(resultSet.next()){
                    String count = resultSet.getString(1);
                    if(!count.trim().equals("0")){
                        String err ="ERROR DB '"+dbName+"' already exists! ." +
                            "\nThis tool is only intended for installing new DBs." +
                            "\nThis needs human intervention.." +
                            "\nHint: " + sql;
                        throw new IllegalStateException(err);
                    }
                } else {
                    log(failed);
                    throw new IllegalStateException(failed);
                }
            }catch( SQLException sqx){
                log(failed, sqx);
                throw new IllegalStateException(failed+"\n"+sqx.getMessage());
            }
        }

//        public void secureCopyAction(ActionEvent e){
//            log("Action: " + e.getActionCommand() );
//            try{
//                Thread thread = new Thread(
//                    new Runnable(){
//                        public void run(){
//                            secureCopyFile(
//                                txtInputPathName.getText(),
//                                smbCopyDests.get(destsCombo.getSelectedItem()),
//                                txtUserName.getText(),
//                                txtPassword.getPassword(),
//                                progressBar
//                            );
//                        }
//                    }
//                );
//                thread.start();
//            }catch( Throwable t ){
//                progressBar.setString("Secure copy failed");
//                String err = String.format("Failed secure copy of '%s' to '%s' (ie %s)",
//                    txtInputPathName.getText(),
//                    destsCombo.getSelectedItem(),
//                    smbCopyDests.get(destsCombo.getSelectedItem())
//                );
//                log(err, t);
//                popErrorDialog(err+"\n"+t.getMessage());
//            }
//        }

        // Check:
        // - DB has been installed
        // - ID is not already mapped in the table
        // - ID is subsequently mapped correctly
        // ...
        void mapDatabaseAction(ActionEvent e, String uncDest, String inputPathname){
            log(String.format("Action: '%s', dest: '%s'", e.getActionCommand(), uncDest ) );

            try{
                final String mdfFileName = new File(inputPathname).getName();
                final String dbName = FilenameUtils.getBaseName(mdfFileName);

                final Connection conn = connectDB(uncDest);
                if(conn != null ){
                    log("Connected to DB for dest: " + uncDest);
                    String dbId = getDBId(dbName,conn);
                    if(dbId != null){
                        int dbIdInt;
                        try{
                            dbIdInt = Integer.valueOf(dbId);
                            log(String.format("DB exists DB_ID('%s')='%s'- Check we can add it to RMS version map", dbName, dbId));
                            verifyDbNotAlreadyMapped(dbIdInt, dbName, conn);
                            lockGui();
                            progressBar.setString("***Adding DB rights..***");
                            progressBar.setValue(0);
                            setupRightsNewStyle(dbName, conn); // Turns out this adds mappings to rms vers 11,13 and 15
                            progressBar.setString("Added DB rights");
                            //  addRMSVersionMapping(11, dbIdInt, dbName, conn);
                            //  addRMSVersionMapping(13, dbIdInt, dbName, conn);
                            //  addRMSVersionMapping(15, dbIdInt, dbName, conn);
                        }catch(NumberFormatException nfe){
                            throw  new IllegalStateException(
                                String.format("ERROR: Pls copy file/attach db first.. DB_ID('%s')='%s'", dbName, dbId),
                                nfe
                            );
                        } finally {
                            unlockGui();
                        }
                    }else{ // dbId null
                        throw  new IllegalStateException(
                            String.format("ERROR: Pls copy file/attach db first.. DB_ID('%s')=null", dbName)
                        );
                    }
                } else { // conn null
                    throw new IllegalArgumentException("Failed to connect to DB");
                }
            }catch(Throwable throwable){
                progressBar.setString("!!!Error adding rights!!!");
                log( throwable.getMessage() );
//                popErrorDialog(throwable.getMessage()); // causes extra error dialog that delays the email
                throw throwable;
            }
        }

        private String getDBId(String dbName, Connection conn){
            String sql = String.format("SELECT DB_ID('%s')",dbName);
            try (final Statement statement = conn.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                if(resultSet.next()){
                    String id = resultSet.getString(1);
                    return id;
                } else {
                    log("ERROR Database does not exist: '"+dbName+"'");
                    return null;
                }
            }catch( SQLException sqx){
                String msg ="FAILED to query DB_ID ("+sql+")";
                log(msg, sqx);
                throw  new IllegalStateException(msg,sqx);
            }
        }
    }

    // Usecase - we've copied the DB file and attached (created the DB) and got its id
    // Before we map it to compatible RMS versions, check it's not already listed
    //
    private void verifyDbNotAlreadyMapped(int dbIdInt, String dbName, Connection conn){
        log(String.format("Doing verifyDbNotAlreadyMapped dbIdInt=%d, dbName=%s", dbIdInt, dbName));
        final String sql = String.format(
            "select count(*) from RAT_loss_tables.dbo.tbl_lkp_rlversion where dbid = %d",
            dbIdInt
        );
        final String failed ="FAILED to query Artisan RMS-version map: ["+sql+"]";
        try (final Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if(resultSet.next()){
                String count = resultSet.getString(1);
                if(!count.trim().equals("0")){
                    String err ="ERROR DB_ID '"+dbIdInt+"' already listed in Artisan RMS-versions map." +
                        "\nEither DB already attached or it was later deleted but mappings left in tbl_lkp_rlversion." +
                        "\nThis needs human intervention.." +
                        "\nHint: " + sql;
                    throw new IllegalStateException(err);
                }
            } else {
                log(failed);
                throw new IllegalStateException(failed);
            }
        }catch( SQLException sqx){
            log(failed, sqx);
            throw new IllegalStateException(failed+"\n"+sqx.getMessage());
        }
    }
    public void setupRightsNewStyle(String dbName, Connection conn) {
        log(String.format("Doing setupRightsNewStyle dbName=%s", dbName));
        try(Statement statement = conn.createStatement()){
          final String sql ="EXEC INDY_LEV.DBO.PROC_PERMS_BATCH @param_dbname_substring = '" + escapeSQL(dbName) + "'";
          statement.execute(sql);
          log("OK: User rights (and Artisan mappings to RMS versions) set up for db '" + dbName + "'");
        }catch (SQLException sqe){
            String msg = "FAILED: User rights (and Artisan mappings to RMS versions) for db '" + dbName + "'";
            log(msg, sqe);
            throw new IllegalStateException( msg + "\n" + sqe.getMessage() );
        }
    }

    // Add entries to RMS-version lookup table
    // To map the DB to compatible RMS versions
    // This is not currently used - the setupRights.. method maps the DB to all three RMS versions
    public void addRMSVersionMapping(int rmsVersion, int dbId, String dbName, Connection conn) {
        log(String.format("Doing addRMSVersionMapping rmsVersion=%d, dbIdInt=%d, dbName=%s", rmsVersion, dbId, dbName));
        try(Statement statement = conn.createStatement()){
            statement.execute(
                String.format(
                    "insert into RAT_loss_tables.dbo.tbl_lkp_rlversion (dbid, rl_version, active, update_date) values (%d, %d, 1, getutcdate())",
                    dbId, rmsVersion
                )
            );
            log(String.format("OK Added Artisan mapping of DB '%s' to RMS ver '%d'", dbName, rmsVersion));
        }catch (SQLException sqe){
            String msg = String.format("FAILED to add entry to map DB '%s' to RMS ver '%d'", dbName, rmsVersion);
            log(msg, sqe);
            popErrorDialog(msg + "\n" + sqe.getMessage());
        }
    }


    private static String escapeSQL(final String sql) {
        String escapee =sql.replace("'", "''");
        return escapee;
    }


    class ListenTextFields extends  KeyAdapter {
        @Override
        public void keyTyped(KeyEvent e) {
            super.keyTyped(e);
            checkEnableButtons();
        }
    }

    // MainWindow constructor
    // Installs listeners that react to user activity
    //
    public MainWindow() {
//        attachDatabaseButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                guiActions.attachDBAction(e, uncCopyDests.get( destsCombo.getSelectedItem().toString()) );
//            }
//        });

        createLayoutGUI();

        destsCombo.addActionListener( new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                log("Selected Dest: "+destsCombo.getSelectedItem());
            }
        });

        installDatabaseButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                logWindow.setText("Log output will be added here.\n");
                String destName = destsCombo.getSelectedItem().toString();
                if( batchModel.isEmpty() ){
                    String sourcePathname = txtInputPathName.getText();
                    String extension = FilenameUtils.getExtension(sourcePathname);
                    if ("MDF".equalsIgnoreCase(extension)) {  // NULL safe
                        guiActions.restoreMDFAction(e, destName);
                    } else if ("BAK".equalsIgnoreCase(extension)) {  // NULL safe
                        guiActions.restoreBAKAction(e, destName);
                    } else {
                        popTitledErrorDialog("What are you smoking ?!","P-P-P-Pick up an MDF or BAK file..");
                    }
                }else if(StringUtils.isBlank(txtInputPathName.getText())){ //batch
                    // nb only one file type should be present
                    if( containsFileOfType("mdf",batchModel.elements()) ){
                        guiActions.restoreMDFAction(e,destName);
//                  }else if( containsFileOfType("bak", batchModel.elements()) ){
//                      guiActions.restoreBAKAction(e, destName);
                    }else{
                        popErrorDialog("Sorry, can currently only batch mdf files");
                    }
                } else {
                    popTitledErrorDialog("I'm confused!","Do you want to run a batch or a single file ?");
                }
            }
        });
//        simpleCopyButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                guiActions.simpleCopyAction(e);
//            }
//        });

        // Open file dialog to set input pathname
        fileFolderPickerButton.addActionListener(new ActionListener(){
            public void actionPerformed( ActionEvent e ){
                guiActions.chooseSourceAction(e);
            }
        } );

        //
        deleteButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                int count = batchList.getSelectedIndices().length;

                for( Object selected : batchList.getSelectedValuesList() ){
                    batchModel.removeElement(selected);
                    log("Remove from batch: "+selected);
                }

                log("Cleared "+count+" selected items from batch");
            }
        });

        chkBatchMode.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                guiActions.batchModeAction(e);
            }
        });


//        secureCopyButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                guiActions.secureCopyAction(e);
//            }
//        });

        // Enable copyMDF button when username and source both available
        txtUserName.addKeyListener(new ListenTextFields());
        txtInputPathName.addKeyListener(new ListenTextFields());
        txtPassword.addKeyListener(new ListenTextFields());

//        mapDatabaseButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                guiActions.mapDatabaseAction(e, uncCopyDests.get( destsCombo.getSelectedItem().toString() ));
//            }
//        });

//        restoreBAKButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                guiActions.restoreBAKAction(e, destsCombo.getSelectedItem().toString());
//            }
//        });

//        txtInputPathName.setText("C:\\temp\\AttachRDMTest.mdf");
        populateDestinations();
//        simpleCopyButton.setEnabled(true);
//        secureCopyButton.setEnabled(false);
//        attachDatabaseButton.setEnabled(false);
//        restoreBAKButton.setEnabled(false);
//        mapDatabaseButton.setEnabled(false);

//        progressBar = new JProgressBar(0, 100);
//        mainPanel.add(progressBar,0);
        progressBar.setValue(0);
        progressBar.setMinimum(0);
        progressBar.setMaximum(100);
        progressBar.setStringPainted(true);
        progressBar.setString("Progress");

        normalCursor = mainPanel.getCursor();
    }

    static String getSystemProperty(String key, String defaultValue){
        if(StringUtils.isBlank(key)){
            throw new IllegalArgumentException("ERROR: Blank key supplied to getSystemProperty()");
        }
        String value = System.getProperty(key);

        if( value != null ){
            LOG.info(String.format("Found supplied value for -D%s=%s", key, value));
            return  value;
        } else {
            LOG.info(String.format("Using default value for -D%s=%s", key, defaultValue));
            return defaultValue;
        }
    }

    private void populateDestinations(){
        final String PROD_DB_NAME = "RMS DB";
        final String TEST_DB_NAME = "Test DB";
        final String MY_PC_NAME = "Local temp/dest folder";

        uncCopyDests.put(PROD_DB_NAME, getSystemProperty("UNC_PATH_PROD", Data.UNC_PATH_PROD_DEFAULT));
        uncCopyDests.put(TEST_DB_NAME, getSystemProperty("UNC_PATH_TEST", Data.UNC_PATH_TEST_DEFAULT));
        uncCopyDests.put(MY_PC_NAME,   getSystemProperty("UNC_PATH_MYPC", Data.UNC_PATH_MYPC_DEFAULT));

        smbCopyDests.put(PROD_DB_NAME, getSystemProperty("SMB_URL_PROD",  Data.SMB_URL_PROD_DEFAULT));
        smbCopyDests.put(TEST_DB_NAME, getSystemProperty("SMB_URL_TEST",  Data.SMB_URL_TEST_DEFAULT));
        smbCopyDests.put(MY_PC_NAME,   getSystemProperty("SMB_URL_MYPC",  Data.SMB_URL_MYPC_DEFAULT));

        destsCombo.removeAllItems();
        destsCombo.addItem(PROD_DB_NAME);
        destsCombo.addItem(TEST_DB_NAME);
        destsCombo.addItem(MY_PC_NAME);
    }

    // TODO move to some utility class
    boolean containsFileOfType( String extension, Enumeration<String> elements){
        while( elements.hasMoreElements()  ){
            String element = elements.nextElement();

            if( FilenameUtils.getExtension(element).equalsIgnoreCase(extension) ){
                return true;
            }
        }
        return false;
    }
    boolean containsFileOfDifferentType( String extension, Enumeration<String> elements){
        while( elements.hasMoreElements()  ){
            if( !FilenameUtils.getExtension(elements.nextElement()).equalsIgnoreCase(extension) ){
                return true;
            }
        }
        return false;
    }
//    private String getUncDestination(){
//        String name = destsCombo.getSelectedItem().toString();
//        String uncPath = uncCopyDests.get(destsCombo.getSelectedItem());
//
//        return uncPath;
//    }
    // Enable secure copy button iff source MDF AND username supplied
    // Simple copy button just needs a source
    // Enable unzip button if a zip file selected
    void checkEnableButtons(){

        boolean gotCreds =
            !txtUserName.getText().trim().isEmpty() &&
            !String.valueOf(txtPassword.getPassword()).trim().isEmpty();


        boolean mdfSelected =
            !txtInputPathName.getText().trim().isEmpty() &&
            txtInputPathName.getText().trim().toUpperCase().endsWith(".MDF");

        boolean bakSelected =
            !txtInputPathName.getText().trim().isEmpty() &&
            txtInputPathName.getText().trim().toUpperCase().endsWith(".BAK");

        boolean mdfInBatch = containsFileOfType("MDF", batchModel.elements());
        boolean bakInBatch = containsFileOfType("BAK", batchModel.elements());

//        attachDatabaseButton.setEnabled( gotCreds && mdfSelected );
        installDatabaseButton.setEnabled( gotCreds &&
            chkBatchMode.isSelected() ? (mdfInBatch || bakInBatch) : (mdfSelected || bakSelected)
        );

//        secureCopyButton.setEnabled( gotCreds &&
//            !txtInputPathName.getText().trim().isEmpty()
//        );

//        simpleCopyButton.setEnabled(
//            !txtInputPathName.getText().trim().isEmpty()
//        );

//        restoreBAKButton.setEnabled( gotCreds && bakSelected );
    }

//    String getFirstLine( String text ){
//        if( StringUtils.isBlank(text)){
//            return "";
//        }
//        final int newlinePos = text.indexOf('\n');
//        if( 0 <= newlinePos ){ // -1 == NOT FOUND
//            return text.substring(0,newlinePos+1);
//        }else{
//            return text;
//        }
//    }

    // Simple copy approach (normal FileInputStream etc) only works if destination
    // network path already connected with credentials eg via explorer, when 'net use' command shows eg:
    //
    //    C:\temp\dest>net use
    //    Status       Local     Remote                    Network
    //    ---------------------------------------------------------
    //    OK                     \\RAT-SQL-APPS-TEST\x$    Microsof..
    //    The command completed successfully.
    //
    // Basic approach FAILS if net share NOT already connected.
    // The JCIFS library approach needs credentials.
    // That's useful to allow IT Apps members to use a GUI without needing to be familiar with the cmd line.
    // Also useful for business users if we are allowed to give a user/pass to Bullock & co..
    //
    private void copyFile(final String srcPathname, final String destDirPath, JProgressBar progress) throws Exception{
        log("copyFile: '" + srcPathname + "', destDirPath: '" + destDirPath + "'");
        assert StringUtils.isNotBlank(srcPathname) && StringUtils.isNotBlank(destDirPath);

        final File srcFile = new File(srcPathname);
        final File destDirFile = new File(destDirPath);
        try{
            if(progress != null){
                progress.setString("Copying file");
                progress.setValue(0);
            }
            lockGui();
            if(!srcFile.isFile()){
                throw new IllegalArgumentException(String.format( "NO SUCH FILE: '%s' (pls choose a valid source srcFile)", srcPathname ));
            }
            if( !destDirFile.isDirectory() ){
                throw new IllegalArgumentException( String.format( "NO DIR: '%s' (network shares may need login)", destDirFile ) );
            }

            final InputStream inputStream = new FileInputStream(srcFile);
            final OutputStream outputStream = new BufferedOutputStream(
                    new FileOutputStream(
                            new File(destDirPath, srcFile.getName())
                    ),
                    1000000
            );
            if( progress != null ){                     // update progress bar
                long length = srcFile.length();
                log("src file len: " + length + " bytes, copying in 10M chunx..");
                byte[] buffer = new byte[1024*1024*10]; // adjust if you want
                long totalBytesRead = 0;
                int bytesRead=0;
                while ((bytesRead = inputStream.read(buffer)) != -1){
                    outputStream.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                    int percentage = (int)(totalBytesRead*100/length);

                    // Is it worth checking for a large enough change ?
                    progress.setValue(Math.min(percentage, 100));
                }
                progress.setString("File copied OK");
                inputStream.close();
                outputStream.flush();
                outputStream.close();

            }else{ //slurp it all approach
                IOUtils.copy(inputStream, outputStream);
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(outputStream);
            }

            log("OK copied srcFile: '" + srcPathname + "', to: '" + destDirPath + "'");
        }catch(Exception e){
            String err = "Failed to copy " + srcPathname;
            progress.setString(err);
            log(err, e);
//            popErrorDialog(err + "\n" + e.getMessage());
            throw e;
        }finally {
            unlockGui();
        }
    }//copyFile()

    private void secureCopyFile(String srcPathname, String destDirPath, String user, char[] pass, JProgressBar progress) throws Exception{
        log(String.format("secureCopyFile: '%s', destDirPath: '%s', user: '%s' ", srcPathname, destDirPath, user));
        assert StringUtils.isNotBlank(srcPathname) && StringUtils.isNotBlank(destDirPath) && StringUtils.isNotBlank(user);

        if(System.getProperty("jcifs.properties") == null ){
            System.setProperty("jcifs.properties", "jcifs.properties");
        }
        log("Using -Djcifs.properties='"+System.getProperty("jcifs.properties")+"'");
        log("Note: CWD=" + new File(".").getAbsolutePath());

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("RAT-ALLIANZ",user, String.valueOf(pass));

        final File srcFile = new File(srcPathname);

        try{
            final SmbFile destSmbDir = new SmbFile(destDirPath, auth);

            if(!srcFile.isFile()){
                throw new IllegalArgumentException(String.format( "NO SUCH FILE: '%s' (pls choose a valid source file)", srcPathname ));
            }
            boolean isDir = false;
            try{
                isDir = destSmbDir.isDirectory();
            }catch(NullPointerException npe){
                // NullPointerException if not SMB url
            }
            if( !isDir ){
                throw new IllegalArgumentException( String.format( "NO DIR: '%s' (may need login for network shares)", destDirPath ) );
            }

            if(!destDirPath.endsWith("/")){
                destDirPath += "/";
            }

            final SmbFile destSmbFile = new SmbFile(destDirPath+srcFile.getName(),auth);
            assert !destSmbFile.isDirectory();
            //    destSmbFile.createNewFile();  // Fails if file already exists, could use this to prevent overwriting ?
            //    assert destSmbFile.canWrite();
            final InputStream inputStream = new FileInputStream(srcFile);
            final OutputStream outputStream = new BufferedOutputStream(
                new SmbFileOutputStream( destSmbFile ),
                1000000
            );

            lockGui();
            if( progress != null ){                     // update progress bar
                progress.setString("Secure copying file");
                progressBar.setValue(0);
                long length = srcFile.length(); int chunk=1024*1024*10;
                log("src file len: " + length + " bytes, copying in approx " + (length/chunk) + " 10M chunx..");
                byte[] buffer = new byte[chunk]; // adjust if you want
                long totalBytesRead = 0;
                int bytesRead=0;
                while ((bytesRead = inputStream.read(buffer)) != -1)
                {
                    outputStream.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                    int percentage = (int)(totalBytesRead*100/length);
                    progress.setValue(Math.min(percentage, 100));
                }
                progress.setString("File secure copied");
                inputStream.close();
                outputStream.flush();
                outputStream.close();

            }else{ //slurp it all approach
                IOUtils.copy(inputStream, outputStream);
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(outputStream);
            }
            unlockGui();
            log(String.format("OK secure copied [%s] to [%s]", srcPathname, destDirPath));
        }catch(Throwable throwable){
            if( progress != null){
                progress.setString("Secure copy failed");
            }
            String err = String.format("Failed secure copy of '%s' to '%s'", srcPathname, destDirPath );
            log(err, throwable);
            popErrorDialog(err+"\n"+throwable.getMessage()); //TODO rethrow ?
            throw throwable;
        }
    }

    private static File getLocalSqlServerLdfFileDir( String destName ) {
        return new File(
            destName.startsWith("Local")
                ? "C:/Program Files/Microsoft SQL Server/MSSQL11.SQLEXPRESS/MSSQL/DATA"
                : getSystemProperty("LocalLdfDir", Data.DEFAULT_LOCAL_LDF_DIR_ON_SQL_SERVER)
        );
    }

    private static File getLocalSqlServerMdfFileDir( String destName ) {
        return new File(
            destName.startsWith("Local")
                ? "C:/Program Files/Microsoft SQL Server/MSSQL11.SQLEXPRESS/MSSQL/DATA"
                : getSystemProperty("LocalMdfDir", Data.DEFAULT_LOCAL_MDF_DIR_ON_SQL_SERVER)
        );
    }

    private void loadJDBCDriver(String name) throws ClassNotFoundException {
        Class.forName(name);
        log("Loaded JDBC Driver: " + name);
    }
    private void log(String msg){
        LOG.info(msg);
        logWindow.setText(
                logWindow.getText() + "\n" + msg );
    }
    private void log(String msg, Throwable throwable){
        LOG.info(msg, throwable);
        logWindow.setText(
            logWindow.getText() + "\n" + msg + "\n" + throwable.getMessage()  );
    }

    // IDEA uses a main() for a form instead of an init() or just doing it in the ctor
    //
    public static void main(String[] args) {
        JFrame frame = new JFrame("Install RMS database");
        frame.setContentPane(new MainWindow().mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
