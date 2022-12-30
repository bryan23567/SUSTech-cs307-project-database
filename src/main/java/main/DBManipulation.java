package main;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import main.interfaces.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBManipulation implements IDatabaseManipulation {
    private final DataSource source;

    public DBManipulation(@NotNull String database, @NotNull String root, @NotNull String pass) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + database);
        config.setUsername(root);
        config.setPassword(pass);
        source = new HikariDataSource(config);
        createTablesAndFunctions();
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
                Type varchar(100),
                check (Code<>N'')
            );
                        
            create table export
            (
                Id  serial not null primary key,
                City_Id    serial not null,
                Tax        numeric(20, 8),
                Officer_id int,
                FOREIGN KEY (City_Id) references City (Id),
                FOREIGN KEY (Officer_id) references staffs (Id)
            );
            create table import
            (
                Id serial not null primary key,
                City_Id serial not null ,
                Tax numeric(20, 8),
                Officer_id int,
                FOREIGN KEY (City_Id) references City (Id),
                FOREIGN KEY (Officer_id) references staffs (Id)
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
                        
            create or replace function check_item(item_name varchar(255), officer varchar(255), success boolean) returns bool as
            $$
            declare
                officer_id0 int := (select id
                                    from staffs
                                    where name = officer);
                ioid        int;
                item_row    record;
                is_import   boolean;
            begin
                select item_id, item_state, export_id, import_id
                into item_row
                from shipping
                         join item i on i.id = shipping.item_id
                where i.name = item_name;
                        
                case (item_row.item_state)
                    when 'Export Checking' then is_import := false;
                                                ioid := item_row.export_id;
                                                if (success) then
                                                    update shipping
                                                    set item_state = 'Packing to Container'
                                                    where item_id = item_row.item_id;
                                                else
                                                    update shipping
                                                    set item_state = 'Export Checking Fail'
                                                    where item_id = item_row.item_id;
                                                end if;
                    when 'Import Checking' then is_import := true;
                                                ioid := item_row.import_id;
                                                if (success) then
                                                    update shipping
                                                    set item_state = 'From-Import Transporting'
                                                    where item_id = item_row.item_id;
                                                else
                                                    update shipping
                                                    set item_state = 'Import Checking Fail'
                                                    where item_id = item_row.item_id;
                                                end if;
                    else return false;
                    end case;
                        
                if (is_import) then
                    update import set officer_id = officer_id0 where id = ioid;
                else
                    update export set officer_id = officer_id0 where id = ioid;
                end if;
                        
                return true;
            exception
                when others then return false;
            end;
            $$ language plpgsql;
            """;

    private void createTablesAndFunctions() {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(CREATE_TABLES)) {
                ps.executeUpdate();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    private static final String CHECK_LOG = "SELECT type FROM staffs WHERE name = ? AND password = ? AND type = ?";

    public boolean checkLog(@NotNull LogInfo log, @Nullable LogInfo.StaffType... perms) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(CHECK_LOG)) {
                ps.setString(1, log.name());
                ps.setString(2, log.password());
                ps.setString(3, log.type().toString());
                if (ps.executeQuery().next()) {
                    for (LogInfo.StaffType perm : perms) {
                        if (perm == log.type())
                            return true;
                    }
                }
                return false;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }


    public boolean checkLog(@NotNull LogInfo log,  List<LogInfo.StaffType> type) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(CHECK_LOG)) {
                ps.setString(1, log.name());
                ps.setString(2, log.password());
                ps.setString(3, log.type().toString());
                if (ps.executeQuery().next()){
                    for (LogInfo.StaffType logType:
                            type) {
                        if (logType == log.type())
                            return true;

                    }
                }
                return false;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }

    @SuppressWarnings("unchecked cast")
    public <T> T[] get(@NotNull String sql, @NotNull Class<T> clazz, @Nullable Object... args) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                if (args != null) {
                    for (int i = 0; i < args.length; i++) {
                        ps.setObject(i + 1, args[i]);
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    List<T> list = new ArrayList<>();
                    while (rs.next()) {
                        list.add((T) rs.getObject(1));
                    }
                    return list.toArray((T[]) Array.newInstance(clazz, list.size()));
                }
            }
        } catch (SQLException | ClassCastException exception) {
            exception.printStackTrace();
            return null;
        }
    }
    public double getImportTaxRate(LogInfo log, String city, String itemClass) {
        String sql="select round(avg(tax/item.price),5) rate from item,import where import.id=item.id and city_id=(select id from city where city.name=?) and item.class=?";
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, city);
                ps.setString(2, itemClass);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return -1;
                }
                return rs.getDouble("rate");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return -1;
        }
    }
    private boolean updateItemState(ItemState itemState,int shippingId){
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement("update shipping set item_state = ? where id = ?")) {
                ps.setString(1,itemState.name());
                ps.setInt(2,shippingId);
                ps.executeUpdate();
                return true;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }
    private int getInt(@NotNull String sql, @Nullable Object... args) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }

                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return -1;
        }
    }

    private double getDouble(@NotNull String sql, @Nullable Object... args) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }

                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getDouble(1);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return -1;
        }
    }

    private boolean getBoolean(@NotNull String sql, @Nullable Object... args) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }

                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getBoolean(1);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }

    private boolean update(@NotNull String sql, @Nullable Object... args) {
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }




    @Override
    public boolean loadItemToContainer(LogInfo log, String itemName, String containerCode) {
        return false;
    }
    private static final String FIND_ITEM = "select Shipping.Id as id,Name,Item_State from shipping inner join Item I on Shipping.Item_Id = I.Id where Container_Id = (select id from container where Code = ?);";
    @Override
    public boolean loadContainerToShip(LogInfo log, String shipName, String containerCode) {

        if (!checkLog(log,  LogInfo.StaffType.CompanyManager)) {
            return false;
        }

        ShipInfo gSi = getShipInfo(log,shipName);
        if (gSi.sailing()){
            return false;
        }

        if (!gSi.owner().matches(getStaffInfo(log, log.name()).company())){
            return false;
        }
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(FIND_ITEM)) {
                ps.setString(1,containerCode);
                ResultSet rs =ps.executeQuery();
                if (rs.next()){
                    do {
                        if (rs.getString("item_state").matches(ItemState.PackingToContainer.name())){
                            if (getStaffInfo(log,getItemInfo(log,rs.getString("name")).retrieval().courier()).company().matches(gSi.owner())){
                                if (updateItemState(ItemState.PackingToContainer,rs.getInt("id"))){
                                    return true;
                                }
                            }
                        }
                    }while (rs.next());
                }
                return false;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }

    }
    private static final String FIND_SHIPPING_BY_SHIP = "Shipping.Id as id,Item_State from shipping inner join ship on Shipping.Ship_Id = Ship.Id where Ship.Name = ?;";
    @Override
    public boolean shipStartSailing(LogInfo log, String shipName) {
        if (!checkLog(log,  LogInfo.StaffType.CompanyManager)) {
            return false;
        }
        if (!getShipInfo(log,shipName).owner().matches(getStaffInfo(log, log.name()).company())){
            return false;
        }
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(FIND_SHIPPING_BY_SHIP)) {
                ps.setString(1,shipName);
                ResultSet rs =ps.executeQuery();
                while (rs.next()){
                    if (rs.getString("item_state").matches(ItemState.WaitingForShipping.name())){
                        return updateItemState(ItemState.Shipping,rs.getInt("id"));
                    }
                }
            }
            return false;
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }
    }
    private static final String FIND_SHIPPING_BY_ITEM = "select foo.Id as id,Name,Item_State from ship inner join (select S.Item_Id,Item_State,Ship_Id,S.Id from item inner join Shipping S on Item.Id = S.Item_Id where Name = ? )as foo on foo.Ship_Id = Ship.Id;";
    @Override
    public boolean unloadItem(LogInfo log, String itemName) {
        if (!checkLog(log,  LogInfo.StaffType.CompanyManager)) {
            return false;
        }


        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(FIND_SHIPPING_BY_ITEM)) {
                ps.setString(1,itemName);
                ResultSet rs =ps.executeQuery();

                while (rs.next()){
                    if (!getShipInfo(log,rs.getString("name")).owner().matches(getStaffInfo(log, log.name()).company())){
                        return false;
                    }
                    if (rs.getString("item_state").matches(ItemState.WaitingForShipping.name())){
                        return updateItemState(ItemState.UnpackingFromContainer,rs.getInt("id"));
                    }
                }
            }
            return false;
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return false;
        }

    }

    private static final String SET_ITEM_WAIT_FOR_CHECKING = """
            update shipping
            set item_state = 'Import Checking'
            where item_id in (select id from item where name = ?)
              and item_state = 'Unpacking from Container';
            """;

    @Override
    public boolean itemWaitForChecking(LogInfo log, String item) {
        if (!checkLog(log, LogInfo.StaffType.CompanyManager)) {
            return false;
        }

        return update(SET_ITEM_WAIT_FOR_CHECKING, item);
    }

    public static final String INSERT_ITEM = "insert into item(name,class,price) values(?,?,?) ";

    @Override
    public double getExportTaxRate(LogInfo log, String city, String itemClass) {
        String sql="select round(avg(tax/item.price),5) rate from item,export where export.id=item.id and city_id=(select id from city where city.name=?) and item.class=?";
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, city);
                ps.setString(2, itemClass);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return -1;
                }
                return rs.getDouble("rate");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return -1;
        }
    }

    public static int count = 1;

    @Override
    public boolean newItem(LogInfo log, ItemInfo item) {
        if (!checkLog(log,  List.of(LogInfo.StaffType.Courier))) {
            System.out.println(1);
            return false;
        }
        System.out.println(count++);

        if((!item.retrieval().city().equals(item.delivery().city()))&&(!item.export().city().equals(item.$import().city()))) {
            double exportTax = getExportTaxRate(log,item.export().city(), item.$class());
            double importTaxRate = getImportTaxRate(log,item.$import().city(), item.$class());

            if((int)(item.export().tax()/item.price()*100)==((int)(exportTax*100))
                    &&(int)(item.$import().tax()/item.price()*100)==((int)(importTaxRate*100))) {

                try (Connection connection = source.getConnection()) {

                    PreparedStatement INSItem = connection.prepareStatement(
                            "insert into item(name,class,price) values(?,?,?)"
                    );
                    INSItem.setString(1, item.name());
                    INSItem.setString(2, item.$class());
                    INSItem.setDouble(3, item.price());
                    INSItem.executeUpdate();
                    System.out.println(11);

                    PreparedStatement SEItem = connection.prepareStatement(
                            "select id from item where name = ?"
                    );
                    SEItem.setString(1, item.name());
                    ResultSet rs8 = SEItem.executeQuery();
                    if(!rs8.next()){
                        return false;
                    }
                    int itemID = rs8.getInt("id");
                    System.out.println(1101);

                    PreparedStatement SEExportCityID = connection.prepareStatement(
                            "select id from city where name = ?"
                    );
                    SEExportCityID.setString(1, item.export().city());
                    ResultSet rs9 = SEExportCityID.executeQuery();

                    if(!rs9.next()){
                        return false;
                    }

                    int ExportCityID = rs9.getInt("id");
                    System.out.println(1102);

                    PreparedStatement SEImportCityID = connection.prepareStatement(
                            "select id from city where name = ?"
                    );
                    SEImportCityID.setString(1, item.$import().city());
                    ResultSet rs10 = SEImportCityID.executeQuery();
                    if(!rs10.next()){
                        return false;
                    }
                    int ImportCityID = rs10.getInt("id");
                    System.out.println(1103);

                    PreparedStatement SECourierID = connection.prepareStatement(
                            "select Id from staffs  where Name = ? and Password = ? and Type = ?"
                    );
                    SECourierID.setString(1, log.name());
                    SECourierID.setString(2, log.password());
                    SECourierID.setString(3, LogInfo.StaffType.Courier.name());
                    ResultSet rs3 = SECourierID.executeQuery();

                    if(!rs3.next()){
                        return false;
                    }

                    int courierID = rs3.getInt("Id");
                    System.out.println(15);


                    PreparedStatement INSExport = connection.prepareStatement(
                            "insert into export(City_Id,Tax,Officer_id) values( ?,?,?)"
                    );
                    INSExport.setInt(1, ExportCityID);
                    INSExport.setDouble(2, item.export().tax());
                    INSExport.setInt(3,courierID);
                    INSExport.executeUpdate();
                    System.out.println(111);

                    PreparedStatement INSImport = connection.prepareStatement(
                            "insert into import(City_Id,Tax,Officer_id) values( ?,?,?)"
                    );
                    INSImport.setInt(1, ImportCityID);
                    INSImport.setDouble(2, item.$import().tax());
                    INSImport.setInt(3,courierID);
                    INSImport.executeUpdate();
                    System.out.println(112);

                    PreparedStatement SEExportID = connection.prepareStatement(
                            "select max(id) as mid from export  group by City_id having City_id = ?  "
                    );
                    SEExportID.setInt(1, ExportCityID);
                    //SEExportID.setDouble(2,item.export().tax());
                    ResultSet rs6 = SEExportID.executeQuery();

                    if(!rs6.next()){
                        return false;
                    }

                    int ExportID = rs6.getInt("mid");
                    System.out.println(113);



                    PreparedStatement SEImportID = connection.prepareStatement(
                            "select max(id) as mid from import  group by City_id having City_id = ?  "
                    );
                    SEImportID.setInt(1, ImportCityID);
                    //SEImportID.setDouble(2,item.$import().tax());
                    ResultSet rs7 = SEImportID.executeQuery();

                    if(!rs7.next()){
                        return false;
                    }

                    int ImportID = rs7.getInt("mid");
                    System.out.println(114);

                    //insert into shipping(item_id,export_id,import_id,ship_id,container_id,item_state) values((select id from item where name = ?),(select id from export where city_id = (select id from city where name = ?)),(select id from import where city_id = (select id from city where name = ? )),(select id from ship where name = ?),(select id from container where code = ?),? )

                    PreparedStatement psShipping = connection.prepareStatement(
                            "insert into shipping(item_id,export_id,import_id,item_state) values(?,?,?,? )"
                    );
                    psShipping.setInt(1, itemID);
                    psShipping.setInt(2, ExportID);
                    psShipping.setInt(3, ImportID);
                    psShipping.setString(4, "Picking-up");

                    psShipping.executeUpdate();

                    System.out.println(12);

                    PreparedStatement SEShippingID = connection.prepareStatement(
                            "select id from  shipping where item_id = ? "
                    );

                    SEShippingID.setInt(1, itemID);
                    ResultSet rs = SEShippingID.executeQuery();

                    if(!rs.next()){
                        return false;
                    }

                    int shippingID = rs.getInt("id");
                    System.out.println(13);



                    PreparedStatement INSRetrieval = connection.prepareStatement(
                            "insert into Retrieval(Shipping_Id,City_Id,Courier_Id) values(?,?,?)"
                    );

                    INSRetrieval.setInt(1,shippingID);
                    INSRetrieval.setInt(2,ImportCityID);
                    INSRetrieval.setInt(3,courierID);
                    INSRetrieval.executeUpdate();
                    System.out.println(16);

                    PreparedStatement INSDelivery = connection.prepareStatement(
                            "insert into Delivery(Shipping_Id,City_Id,courier_id) values(?,?,?)"
                    );
                    INSDelivery.setInt(1,shippingID);
                    INSDelivery.setInt(2,ImportCityID);
                    INSDelivery.setInt(3,courierID);
                    INSDelivery.executeUpdate();
                    System.out.println(17);

                    return true;

                } catch (SQLException sqlException) {
                    System.out.println(2);
                    System.out.println(sqlException);
                    return false;
                }
            }
            else{
                System.out.println(3);
                return false;
            }

        }
        else {
            System.out.println(4);
            return false;
        }
    }

    @Override
    public boolean setItemState(LogInfo log, String name, ItemState s) {
        if (!checkLog(log,  List.of(LogInfo.StaffType.Courier))) {
            System.out.println(1);
            return false;
        }

        try (Connection connection = source.getConnection()) {

            PreparedStatement SEItem = connection.prepareStatement(
                    "select id from item where name = ?"
            );
            SEItem.setString(1, name);
            ResultSet rs8 = SEItem.executeQuery();
            if (!rs8.next()) {
                return false;
            }
            int itemID = rs8.getInt("id");
            System.out.println(1101);

            PreparedStatement SECourierID = connection.prepareStatement(
                    "select Id from staffs  where Name = ? and Password = ? and Type = ?"
            );
            SECourierID.setString(1, log.name());
            SECourierID.setString(2, log.password());
            SECourierID.setString(3, LogInfo.StaffType.Courier.name());
            ResultSet rs3 = SECourierID.executeQuery();

            if(!rs3.next()){
                return false;
            }

            int courierID = rs3.getInt("Id");
            System.out.println(15);

            PreparedStatement SEShippingInfo = connection.prepareStatement(
                    "select id, item_state from shipping  where item_id = ?"
            );
            SEShippingInfo.setInt(1,itemID);
            ResultSet rs4 = SEShippingInfo.executeQuery();
            if(!rs4.next()){
                return false;
            }
            int shippingID = rs4.getInt("id");
            String itemState = rs4.getString("item_state");

            int rCourierID,dCourierID;

            PreparedStatement SERetrievalInfo = connection.prepareStatement(
                    "select courier_id from retrieval where shipping_id = ?"
            );
            SERetrievalInfo.setInt(1,shippingID);
            ResultSet rs5 =  SERetrievalInfo.executeQuery();
            if(!rs5.next()){
                return false;
            }
            rCourierID = rs5.getInt("courier_id");

            if(courierID == rCourierID){
                if(itemState.equals("Picking-up")){
                    //设置item state   "To-Export Transporting"
                    PreparedStatement UPItemState = connection.prepareStatement("update shipping set item_state = 'To-Export Transporting' where id = ?");
                    UPItemState.setInt(1,shippingID);
                    UPItemState.executeUpdate();
                }
                else if(itemState.equals("To-Export Transporting")){
                    //设置item state  "Export Checking"
                    PreparedStatement UPItemState = connection.prepareStatement("update shipping set item_state = 'Export Checking' where id = ?");
                    UPItemState.setInt(1,shippingID);
                    UPItemState.executeUpdate();
                }
                else return false;
            }else {
                try{
                    PreparedStatement SEDeliveryInfo = connection.prepareStatement(
                            "select courier_id from delivery where shipping_id = ?"
                    );
                    SEDeliveryInfo.setInt(1,shippingID);
                    ResultSet rs6 =  SEDeliveryInfo.executeQuery();
                    if(!rs6.next()){
                        return false;
                    }
                    dCourierID = rs5.getInt("courier_id");
                }catch (SQLException e ){
                    dCourierID = -1;
                }catch (NullPointerException n){
                    dCourierID = -1;
                }

                if(dCourierID==courierID){
                    if(itemState.equals("From-Import Transporting")){
                        //"From-Import Transporting" or "Delivering"
                        PreparedStatement UPItemState = connection.prepareStatement("update shipping set item_state = 'Delivering' where id = ?");
                        UPItemState.setInt(1,shippingID);
                        UPItemState.executeUpdate();
                    }
                    else if(itemState.equals("Delivering")){
                        //"Finish"
                        PreparedStatement UPItemState = connection.prepareStatement("update shipping set item_state = 'Finish' where id = ?");
                        UPItemState.setInt(1,shippingID);
                        UPItemState.executeUpdate();
                    }
                    else return false;
                }
                else if((dCourierID ==-1||dCourierID==rCourierID)&&itemState.equals("From-Import Transporting")){
                    //设置状态以及相应的id
                    //"From-Import Transporting" or "Delivering"
                    PreparedStatement UPItemState = connection.prepareStatement("update shipping set item_state = 'Delivering' where id = ?");
                    UPItemState.setInt(1,shippingID);
                    UPItemState.executeUpdate();

                    PreparedStatement UPDelivery = connection.prepareStatement("update delivery set courier_id = ? where id = ?");
                    UPDelivery.setInt(1,courierID);
                    UPDelivery.setInt(2,shippingID);
                    UPDelivery.executeUpdate();

                }
                else return false;

            }

            return true;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

    }

    public static final String INPUT_COMPANY = "insert into company(name) values(?)  ON CONFLICT DO NOTHING";
    public static final String INPUT_CITY = "insert into city(name) values(?) ON CONFLICT DO NOTHING";
    public static final String INPUT_STAFF = "insert into staffs(name,type,company_id,city_id,gender,age,phone_number,password) values(?,?,(select id from company where name = ? ),(select id from city where name = ? ),?,?,?,?)";
    public static final String INPUT_ITEM = "insert into item(name,class,price) values(?,?,?)";
    public static final String INPUT_RETRIEVAL = "insert into retrieval(shipping_id,city_id,courier_id) values(?,(select id from city where name = ?),(select id from staffs where name = ?))";
    public static final String INPUT_DELIVERY = "insert into delivery(shipping_id,city_id,courier_id) values(?,(select id from city where name = ?),(select id from staffs where name = ?))";
    public static final String INPUT_SHIP = "insert into ship(name,company_id) values(?,(select id from company where name = ? )) ON CONFLICT DO NOTHING";
    public static final String INPUT_CONTAINER = "insert into container(Code,Type) values(?,?)ON CONFLICT DO NOTHING";
    public static final String INPUT_EXPORT = "insert into export(city_id,tax,officer_id) values((select id from city where name = ? ),?,(select id from staffs where name = ? ) )";
    public static final String INPUT_IMPORT = "insert into import(city_id,tax,officer_id) values((select id from city where name = ? ),?,(select id from staffs where name = ? ) )";
    public static final String INPUT_SHIPPING = "insert into shipping(item_id,export_id,import_id,ship_id,container_id,item_state) values(?,?,?,(select id from ship where name = ?), (select id from container where code = ?),? )";

    @Override
    public void $import(String recordsCSV, String staffsCSV) {
        String[] staffInfo = staffsCSV.split("\n+");
        String[] recordsInfo = recordsCSV.split("\n+");

        try (Connection connection = source.getConnection()) {
            PreparedStatement psCompany = connection.prepareStatement(INPUT_COMPANY);
            PreparedStatement psCity = connection.prepareStatement(INPUT_CITY);
            PreparedStatement psItem = connection.prepareStatement(INPUT_ITEM);
            PreparedStatement psShip = connection.prepareStatement(INPUT_SHIP);
            PreparedStatement psContainer = connection.prepareStatement(INPUT_CONTAINER);
            PreparedStatement psExport = connection.prepareStatement(INPUT_EXPORT);
            PreparedStatement psImport = connection.prepareStatement(INPUT_IMPORT);
            PreparedStatement psShipping = connection.prepareStatement(INPUT_SHIPPING);
            PreparedStatement psRetrieval = connection.prepareStatement(INPUT_RETRIEVAL);
            PreparedStatement psDelivery = connection.prepareStatement(INPUT_DELIVERY);
            for (int i = 1; i < 86; i++) {
                //input company
                psCompany.setString(1, recordsInfo[i].split(",\\s*")[16]);
                psCompany.addBatch();
                //input city
                psCity.setString(1, recordsInfo[i].split(",\\s*")[3]);
                psCity.addBatch();
                psCity.setString(1, recordsInfo[i].split(",\\s*")[7]);
                psCity.addBatch();
            }
            //input staff
            PreparedStatement psStaff = connection.prepareStatement(INPUT_STAFF);
            for (int i = 1; i < staffInfo.length; i++) {

                psStaff.setString(4, staffInfo[i].split(",\\s*")[3]);
                psStaff.setString(3, staffInfo[i].split(",\\s*")[2]);
                psStaff.setString(1, staffInfo[i].split(",\\s*")[0]);
                psStaff.setString(2, staffInfo[i].split(",\\s*")[1]);
                psStaff.setString(5, staffInfo[i].split(",\\s*")[4]);
                psStaff.setInt(6, Integer.parseInt(staffInfo[i].split(",\\s*")[5]));
                psStaff.setString(7, staffInfo[i].split(",\\s*")[6]);
                psStaff.setString(8, staffInfo[i].split(",\\s*")[7]);
                psStaff.addBatch();
            }
            for (int i = 1; i < recordsInfo.length; i++) {
                //input ship
                psShip.setString(2, recordsInfo[i].split(",\\s*")[16]);
                if (!recordsInfo[i].split(",\\s*")[15].isBlank()) {
                    psShip.setString(1, recordsInfo[i].split(",\\s*")[15]);
                    psShip.addBatch();
                }
                //input container
                psContainer.setString(1, recordsInfo[i].split(",\\s*")[13]);
                psContainer.setString(2, recordsInfo[i].split(",\\s*")[14]);
                if (!recordsInfo[i].split(",\\s*")[13].isBlank()) {
                    psContainer.addBatch();
                }
            }
            for (int i = 1; i < recordsInfo.length; i++) {

                //input item
                psItem.setString(1, recordsInfo[i].split(",\\s*")[0]);
                psItem.setString(2, recordsInfo[i].split(",\\s*")[1]);
                psItem.setInt(3, Integer.parseInt(recordsInfo[i].split(",\\s*")[2]));
                psItem.addBatch();

                //input export
                psExport.setString(1, recordsInfo[i].split(",\\s*")[7]);
                psExport.setDouble(2, Double.parseDouble(recordsInfo[i].split(",\\s*")[9]));
                psExport.setString(3, recordsInfo[i].split(",\\s*")[11]);
                psExport.addBatch();
                //input import
                psImport.setString(1, recordsInfo[i].split(",\\s*")[8]);
                psImport.setDouble(2, Double.parseDouble(recordsInfo[i].split(",\\s*")[10]));
                psImport.setString(3, recordsInfo[i].split(",\\s*")[12]);
                psImport.addBatch();

            }
            for (int i = 1; i < recordsInfo.length; i++) {
                //input shipping
                psShipping.setInt(1, i);
                psShipping.setInt(2, i);
                psShipping.setInt(3, i);
                psShipping.setString(4, recordsInfo[i].split(",\\s*")[15]);
                psShipping.setString(5, recordsInfo[i].split(",\\s*")[13]);
                psShipping.setString(6, recordsInfo[i].split(",\\s*")[17]);
                psShipping.addBatch();
                //input retrieval
                psRetrieval.setInt(1, i);
                psRetrieval.setString(2, recordsInfo[i].split(",\\s*")[3]);
                psRetrieval.setString(3, recordsInfo[i].split(",\\s*")[4]);
                psRetrieval.addBatch();
                //input Delivery
                psDelivery.setInt(1, i);
                psDelivery.setString(2, recordsInfo[i].split(",\\s*")[5]);
                psDelivery.setString(3, recordsInfo[i].split(",\\s*")[6]);
                psDelivery.addBatch();
            }
            psCompany.executeBatch();
            psCity.executeBatch();
            psStaff.executeBatch();
            psShip.executeBatch();
            psContainer.executeBatch();
            psItem.executeBatch();
            psExport.executeBatch();
            psImport.executeBatch();
            psShipping.executeBatch();
            psRetrieval.executeBatch();
            psDelivery.executeBatch();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    private static final String GET_ALL_ITEMS_AT_PORT = """
            with officer_city_id as (select city_id from staffs where name = ?)
            select it.name
            from shipping
                     join item it on it.id = shipping.item_id
                     join import im on im.id = shipping.import_id
                     join export ex on ex.id = shipping.export_id
            where (im.city_id in (select * from officer_city_id) and item_state = 'Import Checking')
               or (ex.city_id in (select * from officer_city_id) and item_state = 'Export Checking');
            """;
    @Override
    public @Nullable String[] getAllItemsAtPort(@NotNull LogInfo log) {
        if (!checkLog(log, LogInfo.StaffType.SeaportOfficer)) {
            return null;
        }

        return get(GET_ALL_ITEMS_AT_PORT, String.class, log.name());
    }

    private static final String SET_ITEM_CHECK_STATE = "select check_item(?, ?, ?);";
    @Override
    public boolean setItemCheckState(@NotNull LogInfo log, @NotNull String itemName, boolean success) {
        if (!checkLog(log, LogInfo.StaffType.SeaportOfficer)) {
            return false;
        }

        return getBoolean(SET_ITEM_CHECK_STATE, itemName, log.name(), success);
    }

    private static final String GET_COMPANY_COUNT = "SELECT count(*) FROM company";

    @Override
    public int getCompanyCount(@NotNull LogInfo log) {

        if (!checkLog(log,  LogInfo.StaffType.SustcManager)) {
            return -1;
        }
        return getInt(GET_COMPANY_COUNT);
    }

    private static final String GET_CITY_COUNT = "SELECT count(*) FROM city";

    @Override
    public int getCityCount(@NotNull LogInfo log) {
        if (!checkLog(log,  LogInfo.StaffType.SustcManager)) {
            return -1;
        }

        return getInt(GET_CITY_COUNT);
    }

    private static final String GET_COURIER_COUNT = "SELECT count(*) FROM staffs WHERE type = 'Courier'";

    @Override
    public int getCourierCount(@NotNull LogInfo log) {
        if (!checkLog(log,LogInfo.StaffType.SustcManager)) {
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
                   iofficer.name       as import_officer,
                   oofficer.name       as export_officer,
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
                     join staffs iofficer on i.officer_id = iofficer.id
                     join staffs oofficer on o.officer_id = oofficer.id
                     join city icity on i.city_id = icity.id
                     join city ocity on o.city_id = ocity.id
                     join item it on shipping.item_id = it.id
            where it.name = ?;
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
                        ItemState.of(rs.getString("item_state")),
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

    private static final String GET_SHIP_INFO = """
            select sName as name, cName as owner, Item_State
            from shipping
                     inner join (select Ship.Name as sName, ship.Id as sId, Company.Name as cName
                                 from ship
                                          inner join company on Ship.Company_Id = Company.Id
                                 where Ship.Name = ?) as foo on foo.sId = Shipping.Ship_Id;
            """;

    @Override
    public ShipInfo getShipInfo(LogInfo log, String name) {
        if (!checkLog(log, LogInfo.StaffType.SustcManager,LogInfo.StaffType.CompanyManager)) {
            return null;
        }
        try (Connection connection = source.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(GET_SHIP_INFO)) {
                ps.setString(1, name);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    String sName = rs.getString("name");
                    String sCompany = rs.getString("owner");
                    do {
                        if (rs.getString("item_state").matches(ItemState.Shipping.name())){
                            return new ShipInfo(sName, sCompany, true);
                        }
                    }while (rs.next());
                    return new ShipInfo(sName, sCompany, false);

                }
                return null;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }
    }

    @Override
    public ContainerInfo getContainerInfo(LogInfo log, String code) {
        try (Connection connection = source.getConnection()) {
            String sql1="select * from container where code=?;";
            try (PreparedStatement ps = connection.prepareStatement(sql1)) {
                ps.setString(1, code);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                String id = rs.getString("id");
                String type = rs.getString("type");
                String sql2="select * from shipping where container_id=? and item_state='Shipping'";
                final PreparedStatement statement = connection.prepareStatement(sql2);
                statement.setInt(1,Integer.valueOf(id));
                final ResultSet set = statement.executeQuery();
                ContainerInfo containerInfo=null;
                if(type.startsWith("Dry")) return new ContainerInfo(ContainerInfo.Type.Dry,code,!set.next());
                if(type.startsWith("ISO")) return new ContainerInfo(ContainerInfo.Type.ISOTank,code,!set.next());
                if(type.startsWith("Open")) return new ContainerInfo(ContainerInfo.Type.OpenTop,code,!set.next());
                if(type.startsWith("Flat")) return new ContainerInfo(ContainerInfo.Type.FlatRack,code,!set.next());
                if(type.startsWith("Reefer")) return new ContainerInfo(ContainerInfo.Type.Reefer,code,!set.next());
                return containerInfo;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }
    }

    @Override
    public StaffInfo getStaffInfo(LogInfo log, String name) {
        try (Connection connection = source.getConnection()) {
            String sql1="select staffs.name staff_name, staffs.type staff_type,staffs.password staff_pwd, c.name company_name,c2.name city_name,staffs.gender gender,staffs.age age,staffs.phone_number  phone_number from staffs left join company c on staffs.company_id = c.id left join city c2 on c2.id = staffs.city_id where staffs.name=?";
            try (PreparedStatement ps = connection.prepareStatement(sql1)) {
                ps.setString(1, name);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                LogInfo logInfo=null;
                String staff_type = rs.getString("staff_type");
                String staff_pwd = rs.getString("staff_pwd");
                String staff_name = rs.getString("staff_name");
                if (staff_type.startsWith("Seaport")) logInfo=new LogInfo(staff_name, LogInfo.StaffType.SeaportOfficer,staff_pwd);
                if (staff_type.startsWith("Company")) logInfo=new LogInfo(staff_name, LogInfo.StaffType.CompanyManager,staff_pwd);
                if (staff_type.startsWith("SUSTC")) logInfo=new LogInfo(staff_name, LogInfo.StaffType.SustcManager,staff_pwd);
                if (staff_type.startsWith("Courier")) logInfo=new LogInfo(staff_name, LogInfo.StaffType.Courier,staff_pwd);
                System.out.print(rs.getString("gender"));
                return  new StaffInfo(logInfo,rs.getString("company_name"), rs.getString("city_name"), rs.getString("gender").equals("female"),rs.getInt("age"),rs.getString("phone_number"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }
    }
}
