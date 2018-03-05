package bigdata;

import java.io.Serializable;
import java.util.List;

public class Intervale implements Serializable {
	public double latitudeMin;
	public double latitudeMax;
	public double longitudeMin;
	public double longitudeMax;

	
	public Intervale(double latitudeMin, double latitudeMax, double longitudeMin, double longitudeMax) {
		super();
		this.latitudeMin = latitudeMin;
		this.latitudeMax = latitudeMax;
		this.longitudeMin = longitudeMin;
		this.longitudeMax = longitudeMax;
	}
	
	public List<Intervale> creationIntervales(double pasLat, double pasLong) {
		double latMin = -90;
		double latMax = 90;
		double longMin = -180;
		double longMax = -180;
		
		List<Intervale> listIntervalle = null;
		
		for (double i= latMin; i<latMax; i+=pasLat)
			for (double j= longMin; i<longMax; i+=pasLong) {
				
				//listIntervalle.add((listIntervalle.size()), (i, i+pasLat, j, j+pasLat));
			}
		return null;
		
	}
		

	@Override
	public String toString() {
		return "Dem3Cordonnees [latitudeMin=" + latitudeMin + ", latitudeMax=" + latitudeMax + " longitudin=\" + longitudeMin +  longitudeMax=" + longitudeMax +"]";
	}
}
