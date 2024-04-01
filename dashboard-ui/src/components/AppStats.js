import React, { useEffect, useState, useCallback } from 'react';
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null);

    const getStats = useCallback(() => {
        fetch(`http://acit3855-lab6-kafka.westus3.cloudapp.azure.com/processing/stats`)
            .then(res => res.json())
            .then(
                (result) => {
                    console.log("Received Stats");
                    setStats(result);
                    setIsLoaded(true);
                },
                (error) => {
                    setError(error);
                    setIsLoaded(true);
                }
            );
    }, []);

    useEffect(() => {
        const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
        return () => clearInterval(interval);
    }, [getStats]);

    if (error) {
        return (<div className={"error"}>Error found when fetching from API</div>);
    } else if (!isLoaded) {
        return (<div>Loading...</div>);
    } else {
        return (
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
                    <tbody>
                        <tr>
                            <th>Hotel Room</th>
                            <th>Hotel Activity</th>
                        </tr>
                        <tr>
                            <td># Hotel Room Reservations: {stats['num_hotel_room_reservations']}</td>
                            <td># Hotel Activity Reservations: {stats['num_hotel_activity_reservations']}</td>
                        </tr>
                        <tr>
                            <td colSpan="2">Max Hotel Room People: {stats['max_hotel_room_ppl']}</td>
                        </tr>
                        <tr>
                            <td colSpan="2">Max Hotel Activity People: {stats['max_hotel_activity_ppl']}</td>
                        </tr>
                    </tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>
            </div>
        );
    }
}


// import React, { useEffect, useState } from 'react'
// import '../App.css';

// export default function AppStats() {
//     const [isLoaded, setIsLoaded] = useState(false);
//     const [stats, setStats] = useState({});
//     const [error, setError] = useState(null)

// 	const getStats = () => {
	
//         fetch(`http://acit3855-lab6-kafka.westus3.cloudapp.azure.com:8100/stats`)
//             .then(res => res.json())
//             .then((result)=>{
// 				console.log("Received Stats")
//                 setStats(result);
//                 setIsLoaded(true);
//             },(error) =>{
//                 setError(error)
//                 setIsLoaded(true);
//             })
//     }
//     useEffect(() => {
// 		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
// 		return() => clearInterval(interval);
//     }, [getStats]);

//     if (error){
//         return (<div className={"error"}>Error found when fetching from API</div>)
//     } else if (isLoaded === false){
//         return(<div>Loading...</div>)
//     } else if (isLoaded === true){
//         return(
//             <div>
//                 <h1>Latest Stats</h1>
//                 <table className={"StatsTable"}>
// 					<tbody>
// 						<tr>
// 							<th>Hotel Room</th>
// 							<th>Hotel Activity</th>
// 						</tr>
// 						<tr>
// 							<td># Hotel Room Reservations: {stats['num_hotel_room_reservations']}</td>
// 							<td># Hotel Activity Reservations: {stats['num_hotel_activity_reservations']}</td>
// 						</tr>
// 						<tr>
// 							<td colspan="2">Max Hotel Room People: {stats['max_hotel_room_ppl']}</td>
// 						</tr>
// 						<tr>
// 							<td colspan="2">Max Hotel Activity People: {stats['max_hotel_activity_ppl']}</td>
// 						</tr>
// 					</tbody>
//                 </table>
//                 <h3>Last Updated: {stats['last_updated']}</h3>

//             </div>
//         )
//     }
// }
