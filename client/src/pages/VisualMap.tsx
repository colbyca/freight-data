import { MapContainer, TileLayer } from "react-leaflet"
import { StopMarkers } from "../components/StopMarkers"
import { JSX } from "react/jsx-runtime";
import 'leaflet/dist/leaflet.css';
import { MapSidebar } from "../components/MapSidebar";

export const VisualMap: () => JSX.Element = () => {
    return (
        <div id="super-container">
        <div className="layout-container">
        <MapContainer center={[41, -112]} zoom={13} scrollWheelZoom={false}>
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <StopMarkers/>
        </MapContainer>
        </div>
        <MapSidebar/>
        </div>
    )
}