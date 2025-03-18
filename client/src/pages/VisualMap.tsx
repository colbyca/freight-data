import { MapContainer, TileLayer } from "react-leaflet"
import { StopMarkers } from "../components/StopMarkers"
import { JSX } from "react/jsx-runtime";
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import { useState } from "react";

export const VisualMap: () => JSX.Element = () => {
    let [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.STOPS);

    let markers: JSX.Element[];
    if (selectedMode.valueOf() === MapViewType.STOPS) {
        markers = StopMarkers();
    }
    return (
        <div id="super-container">
        <div className="layout-container">
        <MapContainer center={[41, -112]} zoom={13} scrollWheelZoom={false}>
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {markers}
        </MapContainer>
        </div>
        <MapSidebar selectedMode={selectedMode} setSelectedMode={setSelectedMode}/>
        </div>
    )
}