package NoMathExpectation.cs307.project2.interfaces;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBManipulation implements IDatabaseManipulation{
    private final DataSource source;

    public DBManipulation(@NotNull String database, @NotNull String root, @NotNull String pass){
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/" + database);
        config.setUsername(root);
        config.setPassword(pass);
        source = new HikariDataSource(config);
    }

    private static final String CHECK_LOG = "SELECT Type FROM staffs WHERE Name = ? AND password = ? AND type = ?";
    public boolean checkLog(@NotNull LogInfo log, @Nullable LogInfo.StaffType type){
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(CHECK_LOG)) {
                ps.setString(1, log.name());
                ps.setString(2, log.password());
                ps.setString(3, log.type().toString());
                return ps.executeQuery().next() && log.type() == type;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }

    private int getInt(@NotNull String sql) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                return ps.executeQuery().getInt(1);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return -1;
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

    private static final String GET_COMPANY_COUNT = "SELECT count(*) FROM company";
    @Override
    public int getCompanyCount(LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_COMPANY_COUNT);
    }

    private static final String GET_CITY_COUNT = "SELECT count(*) FROM city";
    @Override
    public int getCityCount(LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_CITY_COUNT);
    }

    private static final String GET_COURIER_COUNT = "SELECT count(*) FROM staff WHERE type = 'Courier'";
    @Override
    public int getCourierCount(LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_COURIER_COUNT);
    }

    private static final String GET_SHIP_COUNT = "SELECT count(*) FROM ship";
    @Override
    public int getShipCount(LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_SHIP_COUNT);
    }

    private static final String GET_ITEM_INFO = """
            select retcity.name        as retrival_city,
                   delcity.name        as delivery_city,
                   retcourier.name     as retrival_courier,
                   delcourier.name     as delivery_courier,
                   icity.name          as import_city,
                   ocity.name          as export_city,
                   i.officer           as import_officer,
                   o.officer           as export_officer,
                   i.tax               as import_tax,
                   o.tax               as export_tax,
                   it.name             as item_name,
                   it.class            as item_class,
                   it.price            as item_price,
                   shipping.item_state as item_state
            from shipping
                     join retrieval ret on shipping.id = ret.shipping_id
                     join delivery del on shipping.id = del.shipping_id
                     join city delcity on delcity.id = del.city_id
                     join city retcity on retcity.id = ret.city_id
                     join staff retcourier on ret.courier_id = retcourier.id
                     join staff delcourier on del.courier_id = delcourier.id
                     join import i on shipping.import_id = i.id
                     join export o on shipping.export_id = o.id
                     join city icity on i.city_id = icity.id
                     join city ocity on o.city_id = ocity.id
                     join item it on shipping.item_id = it.id
            where it.name = ?
            """;
    @Override
    public ItemInfo getItemInfo(LogInfo log, String name) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return null;
        }

        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(GET_ITEM_INFO)) {
                ps.setString(1, name);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }

                return new ItemInfo(
                        rs.getString("item_name"),
                        rs.getString("item_class"),
                        rs.getDouble("item_price"),
                        ItemState.valueOf(rs.getString("item_state")),
                        new ItemInfo.RetrievalDeliveryInfo(
                                rs.getString("retrival_city"),
                                rs.getString("retrival_courier")
                        ),
                        new ItemInfo.RetrievalDeliveryInfo(
                                rs.getString("delivery_city"),
                                rs.getString("delivery_courier")
                        ),
                        new ItemInfo.ImportExportInfo(
                                rs.getString("import_city"),
                                rs.getString("import_officer"),
                                rs.getDouble("import_tax")
                        ),
                        new ItemInfo.ImportExportInfo(
                                rs.getString("export_city"),
                                rs.getString("export_officer"),
                                rs.getDouble("export_tax")
                        )
                );
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }
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
