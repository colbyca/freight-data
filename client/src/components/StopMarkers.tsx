import { useEffect, useState } from "react"
import MarkerModel from "../Models/MarkerModel";
import { CircleMarker, Marker } from "react-leaflet";
import { JSX } from "react/jsx-runtime";

export const StopMarkers: () => JSX.Element[] = () => {
    let [markers, setMarkers] = useState<MarkerModel[]>([]);
    useEffect(() => {
        let additionList: MarkerModel[] = [];
        
        for (let index = 0; index < 5; index++) {
            additionList[index] = new MarkerModel(40.74404335285939 + (index / 100000), -111.89270459860522 + (index / 10000), "red")
        }

        setMarkers(additionList);
    }, []);
    Marker
    
    return (
        markers!.map(marker => {
            return (
                <CircleMarker center={[marker.latitude, marker.longitude]} pathOptions={{color: marker.color}}/>
            )
        })
    )
}