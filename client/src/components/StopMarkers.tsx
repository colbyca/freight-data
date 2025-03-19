import { useEffect, useState } from "react"
import MarkerModel from "../Models/MarkerModel";
import { CircleMarker, Marker } from "react-leaflet";
import { JSX } from "react/jsx-runtime";

interface MarkerProps {
    markers: MarkerModel[],
}

export const StopMarkers = ({markers} : MarkerProps) => {
    return (
        markers!.map(marker => {
            return (
                <CircleMarker center={[marker.latitude, marker.longitude]} pathOptions={{color: marker.color}}/>
            )
        })
    )
}