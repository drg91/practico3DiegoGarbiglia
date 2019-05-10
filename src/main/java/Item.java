
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Item {

    private String id;
    private String siteId;
    private String title;
    private String subtitle;
    private Integer sellerId;
    private String categoryId;
    private Integer price;
    private String currencyId;
    private Integer availableQuantity;
    private String condition;
    private List<String> pictures = null;
    private Boolean acceptsMercadopago;
    private String status;
    private String dateCreated;
    private String lastUpdated;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSubtitle() {
        return subtitle;
    }

    public void setSubtitle(String subtitle) {
        this.subtitle = subtitle;
    }

    public Integer getSellerId() {
        return sellerId;
    }

    public void setSellerId(Integer sellerId) {
        this.sellerId = sellerId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public String getCurrencyId() {
        return currencyId;
    }

    public void setCurrencyId(String currencyId) {
        this.currencyId = currencyId;
    }

    public Integer getAvailableQuantity() {
        return availableQuantity;
    }

    public void setAvailableQuantity(Integer availableQuantity) {
        this.availableQuantity = availableQuantity;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<String> getPictures() {
        return pictures;
    }

    public void setPictures(List<String> pictures) {
        this.pictures = pictures;
    }

    public Boolean getAcceptsMercadopago() {
        return acceptsMercadopago;
    }

    public void setAcceptsMercadopago(Boolean acceptsMercadopago) {
        this.acceptsMercadopago = acceptsMercadopago;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(String dateCreated) {
        this.dateCreated = dateCreated;
    }

    public String getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public String toString() {
        return String.format("Item{id='%s', title='%s', price='%s'}", id, title, price);
    }

//    public Map<String, Object> toDataMap(){
//        Map<String, Object> dataMap = new HashMap<String, Object>();
//        dataMap.put("id", this.id);
//        dataMap.put("siteId", this.siteId);
//        dataMap.put("title", this.title);
//        dataMap.put("subtitle", this.subtitle);
//        dataMap.put("sellerId", this.sellerId);
//        dataMap.put("categoryId", this.categoryId);
//        dataMap.put("price", this.price);
//        dataMap.put("currencyId", this.currencyId);
//        dataMap.put("availableQuantity", this.availableQuantity);
//        dataMap.put("condition", this.condition);
//        dataMap.put("pictures", this.pictures);
//        dataMap.put("acceptsMercadopago", this.acceptsMercadopago);
//        dataMap.put("status", this.status);
//        dataMap.put("dateCreated", this.dateCreated);
//        dataMap.put("lastUpdated", this.lastUpdated);
//        return dataMap;
//    }

}