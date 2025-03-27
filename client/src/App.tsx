import { Outlet } from 'react-router'

function App() {

  return (

    <div className="app">
      <div className="title">Freight Stops</div>
      <div className='container'>
        <Outlet />
      </div>
    </div>

  )
}

export default App
