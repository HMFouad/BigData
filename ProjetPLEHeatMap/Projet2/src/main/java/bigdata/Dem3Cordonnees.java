package bigdata;

import java.io.Serializable;

public class Dem3Cordonnees implements Serializable {
	public double latitude;
	public double longitude;
	public double hauteur;

	public Dem3Cordonnees(double latitude, double longitude, double hauteur) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
		this.hauteur = hauteur;
	}

	@Override
	public String toString() {
		return "Dem3Cordonnees [latitude=" + latitude + ", longitude=" + longitude + ", hauteur=" + hauteur +"]";
	}

}
