export default class MarkerModel {
    latitude: number;
    longitude: number;
    color: String;

    constructor(latitude: number, longitude: number, color: String) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.color = color;
    }
}