package ch.hsr.geohash;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GetGeoHash extends UDF  {
  private Text result = new Text();

  public Text evaluate(Text latitude, Text longitude) {

    if(latitude == null || longitude == null) {
      return null;
    }
    GeoHash geoHash = GeoHash
        .withCharacterPrecision(Double.parseDouble(latitude.toString()), Double.parseDouble(longitude.toString()),
            5);

    result.set(geoHash.toBase32());

    return result;

  }

  public Text evaluate(Text str) {

    if(str == null) {

      return null;

    }

    result.set(StringUtils.strip(str.toString()));

    return result;

  }

}
