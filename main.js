const form = document.querySelector('form');
form.addEventListener('submit', handleSubmit);

/** @param {Event} event */
function handleSubmit(event) {

    const form = event.currentTarget;
    const url = new URL(form.action);
    const formData = new FormData(form);
    const searchParams = new URLSearchParams(formData);

    const fetchOptions = {
        method: form.method,
    };
    
    if (form.method.toLowerCase() === 'post') {
        if (form.enctype === 'multipart/form-data') {
            fetchOptions.body = formData;
        } else {
          fetchOptions.body = searchParams;
        }
    } else {
        url.search = searchParams;
    }

    try{
        fetch(url, fetchOptions);
    } catch(e){
        console.error(e)
    }
    
    event.preventDefault();
}

// // const input = document.querySelector('input[type="file"]');
// // input.addEventListener('change', (e) => {

// //   const fd = new FormData();

// //   // add all selected files
// //   fd.append(e.target.name, e.fil, e.name);  
  
// //   // create the request
// //   const xhr = new XMLHttpRequest();
// //   xhr.onload = () => {
// //     if (xhr.status >= 200 && xhr.status < 300) {
// //         // we done!
// //     }
// //   };
  
// //   // path to server would be where you'd normally post the form to
// //   xhr.open('POST', '/modify', true);
// //   xhr.send(fd);
// // });
// document.getElementById('modifyButton').addEventListener('click', function () {
//     const fileInput = document.getElementById('fileInput');
//     const newFilename = document.getElementById('newFilename').value;

//     if (!fileInput.files.length || !newFilename) {
//         alert('Please select a file and enter a new filename.');
//         return;
//     }

//     const selectedFile = fileInput.files[0];
//     console.log('Selected File:', selectedFile.name); // Log the selected file's name to the console

//     const formData = new FormData();
//     formData.append('file', fileInput.files[0]);

//       // create the request
//     const xhr = new XMLHttpRequest();
//     xhr.onload = () => {
//         if (xhr.status >= 200 && xhr.status < 300) {
//             // we done!
//         }
//     };
  
//   // path to server would be where you'd normally post the form to
//     xhr.open('POST', '/modify', true);
//     xhr.send(formData);


//         // .then(response => response.blob())
//         // .then(blob => {
//         //     const downloadLink = document.createElement('a');
//         //     downloadLink.href = URL.createObjectURL(blob);
//         //     downloadLink.download = newFilename;
//         //     downloadLink.style.display = 'none';
//         //     document.body.appendChild(downloadLink);
//         //     downloadLink.click();
//         //     document.body.removeChild(downloadLink);
//         // })
//         // .catch(error => {
//         //     console.error('An error occurred:', error);
//         // });
// });