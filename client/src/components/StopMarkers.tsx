import MarkerModel from "../Models/MarkerModel";
import { CircleMarker } from "react-leaflet";

export interface StopMarkerProps {
    markers: MarkerModel[],
}

export const StopMarkers = ({markers} : StopMarkerProps) => {
    return (
        markers!.map(marker => {
            return (
                <CircleMarker center={[marker.latitude, marker.longitude]} pathOptions={{color: marker.color}}/>
            )
        })
    )
}