package bigdata;

import java.io.Serializable;

public class WorldCitiesCordonnees implements Serializable{
	
	public String pays;
	public String ville;
	public String region;
	public double population;
	public double latitude;
	public double longitude;

	public WorldCitiesCordonnees(String pays, String ville, String region, double population,
			double latitude,double longitude) {
		super();
		this.pays = pays;
		this.ville = ville;
		this.region = region;
		this.population = population;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	@Override
	public String toString() {
		return "WorldCitiesCordonnees [pays=" + pays + ", ville=" + ville + 
				", region=" + region + ", population=" + population + ","
				+ " latitude=" + latitude + ", longitude=" + longitude +"]";
	}
	
}
