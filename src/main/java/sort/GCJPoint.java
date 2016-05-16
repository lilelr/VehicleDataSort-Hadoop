package sort;



public class GCJPoint {
    private int longitude;
    private int latitude;


    public void setLongitude(int longitude) {
            this.longitude = longitude;
    }


    public void setLatitude(int latitude) {
            this.latitude = latitude;
    }


    public int getLongitude() {
            return longitude;
    }


    public int getLatitude() {
            return latitude;
    }
    
    public String toString1() {
    	  
    	return (longitude/1024.0/3600)+","+(latitude/1024.0/3600);
    }
    
    public String toString() {
  
    	return longitude+","+latitude;
    }
}
