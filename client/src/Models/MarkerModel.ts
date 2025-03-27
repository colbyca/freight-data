export default class MarkerModel {
    latitude: number;
    longitude: number;
    color: string;

    constructor(latitude: number, longitude: number, color: string) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.color = color;
    }
}