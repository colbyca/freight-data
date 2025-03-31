import { LatLngBounds } from "leaflet";
import React from "react";
import { useMapEvents } from "react-leaflet"

interface MapEventHandlerProps {
    setBounds: React.Dispatch<React.SetStateAction<LatLngBounds>>
}

export const MapEventHandler = ({setBounds} : MapEventHandlerProps) => {
    const map = useMapEvents({ "moveend": () => {
        setBounds(map.getBounds())
    }});
    return null;
}