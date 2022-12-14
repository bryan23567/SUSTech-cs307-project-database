package NoMathExpectation.cs307.project2.interfaces;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBManipulation implements IDatabaseManipulation{
    private final DataSource source;

    public DBManipulation(String database, String root, String pass){
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/" + database);
        config.setUsername(root);
        config.setPassword(pass);
        source = new HikariDataSource(config);
    }

    private static final String CHECK_LOG = "SELECT Type FROM staffs WHERE Name = ? AND password = ? AND type = ?";
    public boolean checkLog(LogInfo log){
        try (Connection conn = source.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(CHECK_LOG)) {
                ps.setString(1, log.name());
                ps.setString(2, log.password());
                ps.setString(3, log.type().toString());
                return ps.executeQuery().next();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }

    @Override
    public double getImportTaxRate(LogInfo log, String city, String itemClass) {
        return 0;
    }

    @Override
    public double getExportTaxRate(LogInfo log, String city, String itemClass) {
        return 0;
    }

    @Override
    public boolean loadItemToContainer(LogInfo log, String itemName, String containerCode) {
        return false;
    }

    @Override
    public boolean loadContainerToShip(LogInfo log, String shipName, String containerCode) {
        return false;
    }

    @Override
    public boolean shipStartSailing(LogInfo log, String shipName) {
        return false;
    }

    @Override
    public boolean unloadItem(LogInfo log, String itemName) {
        return false;
    }

    @Override
    public boolean itemWaitForChecking(LogInfo log, String item) {
        return false;
    }

    @Override
    public boolean newItem(LogInfo log, ItemInfo item) {
        return false;
    }

    @Override
    public boolean setItemState(LogInfo log, String name, ItemState s) {
        return false;
    }

    @Override
    public void $import(String recordsCSV, String staffsCSV) {

    }

    @Override
    public String[] getAllItemsAtPort(LogInfo log) {
        return new String[0];
    }

    @Override
    public boolean setItemCheckState(LogInfo log, String itemName, boolean success) {
        return false;
    }

    @Override
    public int getCompanyCount(LogInfo log) {
        return 0;
    }

    @Override
    public int getCityCount(LogInfo log) {
        return 0;
    }

    @Override
    public int getCourierCount(LogInfo log) {
        return 0;
    }

    @Override
    public int getShipCount(LogInfo log) {
        return 0;
    }

    @Override
    public ItemInfo getItemInfo(LogInfo log, String name) {
        return null;
    }

    @Override
    public ShipInfo getShipInfo(LogInfo log, String name) {
        return null;
    }

    @Override
    public ContainerInfo getContainerInfo(LogInfo log, String code) {
        return null;
    }

    @Override
    public StaffInfo getStaffInfo(LogInfo log, String name) {
        return null;
    }
}
