package main;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import main.interfaces.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.*;

public class DBManipulation implements IDatabaseManipulation {
    private final DataSource source;

    public DBManipulation(@NotNull String database, @NotNull String root, @NotNull String pass) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + database);
        config.setUsername(root);
        config.setPassword(pass);
        source = new HikariDataSource(config);
        createTables();
    }

    private static final String CREATE_TABLES = """
            create table Company
            (
                Id   serial primary key,
                Name varchar(255) not null unique,
                check (Name <>N'')
            );
            create table City
            (
                Id   serial primary key,
                Name varchar(255) not null unique,
                check (Name <>N'')
            );
            create table staffs
            (
                Id serial primary key,
                Name         varchar(255) not null,
                Type       varchar(30)  not null,
                Age         int        not null,
                Company_Id int ,
                City_id int,
                Gender varchar(10),
                Phone_Number varchar(20),
                Password varchar(20),
                FOREIGN KEY (Company_Id) references Company (Id),
                FOREIGN KEY (City_id) references City (Id)
            );
            create table Item
            (
                Id    serial primary key,
                Name  varchar(255)  not null unique,
                Class varchar(255)  not null,
                Price numeric(9, 0) not null,
                check ( Price > 0 and Class <> N'' and Name <>N'')
            );
                        
                        
            create table Ship
            (
                Id      serial primary key,
                Name    varchar(255) not null unique,
                Company_Id int          not null,
                FOREIGN KEY (Company_Id) references Company (Id)
                        
            );
            create table Container
            (
                Id   serial primary key,
                Code varchar(8)  not null unique,
                Type varchar(100)
            );
                        
            create table export
            (
                Id serial not null primary key,
                City_Id serial not null ,
                Tax numeric(15, 8),
                Officer varchar(30),
                FOREIGN KEY (City_Id) references City (Id)
            );
            create table import
            (
                Id serial not null primary key,
                City_Id serial not null ,
                Tax numeric(15, 8),
                Officer varchar(30),
                FOREIGN KEY (City_Id) references City (Id)
            );
                        
            create table Shipping
            (
                Id               serial primary key,
                Item_Id       int  not null,
                Export_Id int  not null,
                Import_Id int  not null,
                Ship_Id             int,
                Container_Id        int,
                Item_State      varchar(50),
                FOREIGN KEY (Export_Id) references export (Id),
                FOREIGN KEY (Import_Id) references import (Id),
                FOREIGN KEY (Item_Id) references item (Id),
                FOREIGN KEY (Ship_Id) references Ship (Id),
                FOREIGN KEY (Container_Id) references Container (Id)
            );
                        
            create table Retrieval
            (
                Id               serial primary key,
                Shipping_Id int ,
                City_Id        serial not null,
                Courier_Id     int    not null,
                FOREIGN KEY (Courier_Id) references staffs (id),
                FOREIGN KEY (City_Id) references City (Id),
                 FOREIGN KEY (Shipping_Id) references Shipping (Id)
            );
            create table Delivery
            (
                Id               serial primary key,
                Shipping_Id int ,
                City_Id         int not null,
                Courier_Id      int,
                FOREIGN KEY (Courier_Id) references staffs (id),
                FOREIGN KEY (City_Id) references City (Id),
                 FOREIGN KEY (Shipping_Id) references Shipping (Id)
            );
            """;
    private void createTables() {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(CREATE_TABLES)) {
                ps.executeUpdate();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    private static final String CHECK_LOG = "SELECT type FROM staff WHERE name = ? AND password = ? AND type = ?";

    public boolean checkLog(@NotNull LogInfo log, @Nullable LogInfo.StaffType type) {
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
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
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

    public static final String INPUT_COMPANY = "insert into company(name) values(?)  ON CONFLICT DO NOTHING";
    public static final String INPUT_CITY = "insert into city(name) values(?) ON CONFLICT DO NOTHING";
    public static final String INPUT_STAFF = "insert into staffs(name,type,company_id,city_id,gender,age,phone_number,password) values(?,?,?,?,?,?,?,?)";
    public static final String INPUT_ITEM = "insert into item(name,class,price) values(?,?,?)";
    public static final String INPUT_SHIP = "insert into ship(name,company_id) values(?,?) ON CONFLICT DO NOTHING";
    public static final String FIND_CITY_BY_NAME = "select id from city where name = ?";
    public static final String FIND_COMPANY_BY_NAME = "select id from company where name = ?";
    @Override
    public void $import(String recordsCSV, String staffsCSV) {
        String[] staffInfo = staffsCSV.split("\n+");
        String[] recordsInfo = recordsCSV.split("\n+");

        try (Connection connection = source.getConnection()) {
            PreparedStatement psCompany = connection.prepareStatement(INPUT_COMPANY);
            PreparedStatement psCity = connection.prepareStatement(INPUT_CITY);
            PreparedStatement psItem = connection.prepareStatement(INPUT_ITEM);
            PreparedStatement psShip = connection.prepareStatement(INPUT_SHIP);
            for (int i = 1; i < recordsInfo.length; i++) {

                psCompany.setString(1, recordsInfo[i].split(",\\s*")[16]);
                psCompany.executeUpdate();
                psCity.setString(1, recordsInfo[i].split(",\\s*")[3]);
                psCity.executeUpdate();
                psCity.setString(1, recordsInfo[i].split(",\\s*")[7]);
                psCity.executeUpdate();
                psItem.setString(1,recordsInfo[i].split(",\\s*")[0]);
                psItem.setString(2,recordsInfo[i].split(",\\s*")[1]);
                psItem.setInt(3, Integer.parseInt(recordsInfo[i].split(",\\s*")[2]));
                psItem.executeUpdate();
                psShip.setString(1,recordsInfo[i].split(",\\s*")[15]);
                //find company by name
                PreparedStatement psFindCompany= connection.prepareStatement(FIND_COMPANY_BY_NAME);
                psFindCompany.setString(1,recordsInfo[i].split(",\\s*")[16]);

                int company_id =0;
                ResultSet companyRs= psFindCompany.executeQuery();

                if (companyRs.next()){
                    company_id = companyRs.getInt("id");
                    psShip.setInt(2,company_id);
                }else{
                    psShip.setNull(2, Types.NULL);
                }
                if(recordsInfo[i].split(",\\s*")[15]!=""){
                    psShip.executeUpdate();
                }
            }
            PreparedStatement psStaff = connection.prepareStatement(INPUT_STAFF);
            for (int i = 1;i<staffInfo.length;i++){
                //find city by name
                PreparedStatement psFindCity= connection.prepareStatement(FIND_CITY_BY_NAME);
                psFindCity.setString(1,staffInfo[i].split(",\\s*")[3]);
                int city_id;
                ResultSet cityRs= psFindCity.executeQuery();
                if (cityRs.next()){
                    city_id = cityRs.getInt("id");
                    psStaff.setInt(4,city_id);
                }else{
                    psStaff.setNull(4, Types.NULL);
                }
                //find company by name
                PreparedStatement psFindCompany= connection.prepareStatement(FIND_COMPANY_BY_NAME);
                psFindCompany.setString(1,staffInfo[i].split(",\\s*")[2]);
                int company_id =0;
                ResultSet companyRs= psFindCompany.executeQuery();
                if (companyRs.next()){
                    company_id = companyRs.getInt("id");
                    psStaff.setInt(3,company_id);
                }else{
                    psStaff.setNull(3, Types.NULL);
                }
                //insert all values
                psStaff.setString(1,staffInfo[i].split(",\\s*")[0]);
                psStaff.setString(2,staffInfo[i].split(",\\s*")[1]);
                psStaff.setString(5,staffInfo[i].split(",\\s*")[4]);
                psStaff.setInt(6, Integer.parseInt(staffInfo[i].split(",\\s*")[5]));
                psStaff.setString(7,staffInfo[i].split(",\\s*")[6]);
                psStaff.setString(8,staffInfo[i].split(",\\s*")[7]);
                psStaff.execute();
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
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
    public int getCompanyCount(@NotNull LogInfo log) {

        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }
        return getInt(GET_COMPANY_COUNT);
    }

    private static final String GET_CITY_COUNT = "SELECT count(*) FROM city";

    @Override
    public int getCityCount(@NotNull LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_CITY_COUNT);
    }

    private static final String GET_COURIER_COUNT = "SELECT count(*) FROM staffs WHERE type = 'Courier'";

    @Override
    public int getCourierCount(@NotNull LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_COURIER_COUNT);
    }

    private static final String GET_SHIP_COUNT = "SELECT count(*) FROM ship";

    @Override
    public int getShipCount(@NotNull LogInfo log) {
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
                     join staffs retcourier on ret.courier_id = retcourier.id
                     join staffs delcourier on del.courier_id = delcourier.id
                     join import i on shipping.import_id = i.id
                     join export o on shipping.export_id = o.id
                     join city icity on i.city_id = icity.id
                     join city ocity on o.city_id = ocity.id
                     join item it on shipping.item_id = it.id
            where it.name = ?
            """;

    @Override
    public @Nullable ItemInfo getItemInfo(@NotNull LogInfo log, @NotNull String name) {
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
