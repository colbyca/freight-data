import MarkerModel from "../Models/MarkerModel";
import { CircleMarker } from "react-leaflet";

export interface StopMarkerProps {
    markers: MarkerModel[],
}

export const StopMarkers = ({markers} : StopMarkerProps) => {
    return (
        markers!.map((marker, index) => {
            return (
                <CircleMarker key={index} center={[marker.latitude, marker.longitude]} radius={5} pathOptions={{color: marker.color}}/>
            )
        })
    )
}