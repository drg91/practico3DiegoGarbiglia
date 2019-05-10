import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Application {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "itemdata";
    private static final String TYPE = "item";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     * @return RestHighLevelClient
     */
    private static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }


    private static Item insertItem(Item item){
        item.setId(item.getId());
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("id", item.getId());
        dataMap.put("siteId", item.getSiteId());
        dataMap.put("title", item.getTitle());
        dataMap.put("subtitle", item.getSubtitle());
        dataMap.put("sellerId", item.getSellerId());
        dataMap.put("categoryId", item.getCategoryId());
        dataMap.put("price", item.getPrice());
        dataMap.put("currencyId", item.getCurrencyId());
        dataMap.put("availableQuantity", item.getAvailableQuantity());
        dataMap.put("condition", item.getCondition());
        dataMap.put("pictures", item.getPictures());
        dataMap.put("acceptsMercadopago", item.getAcceptsMercadopago());
        dataMap.put("status", item.getStatus());
        dataMap.put("dateCreated", item.getDateCreated());
        dataMap.put("lastUpdated", item.getLastUpdated());
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, item.getId())
                .source(dataMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (IOException ex){
            ex.getLocalizedMessage();
        }
        return item;
    }

    private static Item getItemById(String id){
        GetRequest getItemRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getItemRequest);
        } catch (IOException e){
            e.getLocalizedMessage();
        }
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Item.class) : null;
    }

    private static Item updateItemById(String id, Item item){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
                .fetchSource(true);    // Fetch Object after its update
        try {
            String itemJson = objectMapper.writeValueAsString(item);
            updateRequest.doc(itemJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Item.class);
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (IOException e){
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update item");
        return null;
    }

    private static void deleteItemById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (IOException e){
            e.getLocalizedMessage();
        }
    }

    private static String readUrl(String urlString) throws IOException {

        BufferedReader reader = null;

        try {
            URL url = new URL(urlString);
            URLConnection connection = url.openConnection();
            connection.setRequestProperty("Accept","application/json");
            connection.setRequestProperty("User-Agent","Mozilla/5.0");
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(),"UTF-8"));
            int read = 0;
            StringBuffer buffer = new StringBuffer();
            char[] chars = new char[30000];
            while ((read = reader.read(chars)) != -1){
                buffer.append(chars,0,read);
            }
            return buffer.toString();
        }
        finally {
            if(reader != null){
                reader.close();
            }
        }

    }
    private static boolean getSite (String id){
        try{
            readUrl("https://api.mercadolibre.com/sites/" + id);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
    private static boolean getCategory (String id){
        try{
            readUrl("https://api.mercadolibre.com/categories/" + id);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static int menu() {

        int selection;
        Scanner input = new Scanner(System.in);

        System.out.println("\n\nElegir accion a realizar sobre base de datos");
        System.out.println("-------------------------\n");
        System.out.println("1 - Agregar Item");
        System.out.println("2 - Actualizar Item");
        System.out.println("3 - Borrar item");
        System.out.println("4 - Mostrar item");
        System.out.println("5 - Quit");

        selection = input.nextInt();
        return selection;
    }
    public static void main(String[] args) throws IOException {
        boolean salir = true;
        while(salir){
            int userChoice;
            Scanner input = new Scanner(System.in);
            userChoice = menu();

            switch (userChoice) {
                case 1: //Insert

                    System.out.println("Creando nuevo item...");
                    Item item = new Item();

                    System.out.print("Ingresar itemID: ");
                    String in = input. nextLine();
                    item.setId(in);

                    System.out.print("Ingresar SiteID: ");
                    in = input. nextLine();
                    if (!getSite(in)){
                        System.out.println("No se pudo agregar item, SiteId invalido");
                        break;
                    }
                    item.setSiteId(in);

                    System.out.print("Ingresar CategoryID: ");
                    in = input. nextLine();
                    if (!getCategory(in)){
                        System.out.println("No se pudo agregar item, CategoryID invalido");
                        break;
                    }
                    item.setCategoryId(in);

                    System.out.print("Ingresar Titulo: ");
                    in = input. nextLine();
                    item.setTitle(in);

                    System.out.print("Ingresar Precio: ");
                    in = input. nextLine();
                    item.setPrice(new Integer(in));

                    makeConnection();
                    item = insertItem(item);
                    System.out.println("Item Agregado --> " + item);
                    closeConnection();

                    break;

                case 2: //Actualizar

                    System.out.println("Actualizando item...");

                    System.out.print("Ingresar itemID a actualizar: ");
                    in = input. nextLine();
                    Item itemEdit = getItemById(in);

                    System.out.print("Modificar SiteID: ");
                    in = input. nextLine();
                    if (!getSite(in)){
                        System.out.println("No se pudo modificar item, SiteId invalido");
                        break;
                    }
                    itemEdit.setSiteId(in);

                    System.out.print("Modificar CategoryID: ");
                    in = input. nextLine();
                    if (!getCategory(in)){
                        System.out.println("No se pudo modificar item, CategoryID invalido");
                        break;
                    }
                    itemEdit.setCategoryId(in);

                    System.out.print("Modificar Titulo: ");
                    in = input. nextLine();
                    itemEdit.setTitle(in);

                    System.out.print("Modificar Precio: ");
                    in = input. nextLine();
                    itemEdit.setPrice(new Integer(in));


                    makeConnection();
                    updateItemById(itemEdit.getId(), itemEdit);
                    System.out.println("Item updated  --> " + itemEdit);
                    closeConnection();

                    break;
                case 3:  //Delete
                    makeConnection();
                    System.out.print("Ingresar itemID: ");
                    in = input. nextLine();
                    Item itemFromDB = getItemById(in);
                    System.out.println("Borrando " + itemFromDB.getTitle());
                    deleteItemById(itemFromDB.getId());
                    System.out.println("Item Deleted");
                    closeConnection();
                    break;

                case 4: //Get
                    makeConnection();
                    System.out.print("Ingresar itemID a mostrar: ");
                    in = input. nextLine();
                    itemFromDB = getItemById(in);
                    System.out.println(itemFromDB);
                    closeConnection();
                    break;

                case 5:
                    salir = false;
                    break;
            }

        }

    }
}