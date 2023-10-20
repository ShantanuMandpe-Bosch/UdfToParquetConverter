document.getElementById('modifyButton').addEventListener('click', function () {
    const fileInput = document.getElementById('fileInput');
    const newFilename = document.getElementById('newFilename').value;

    if (!fileInput.files.length || !newFilename) {
        alert('Please select a file and enter a new filename.');
        return;
    }

    const selectedFile = fileInput.files[0];
    console.log('Selected File:', selectedFile.name); // Log the selected file's name to the console

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    fetch('/modify', {
        method: 'POST',
        body: formData,
    })
        .then(response => response.blob())
        .then(blob => {
            const downloadLink = document.createElement('a');
            downloadLink.href = URL.createObjectURL(blob);
            downloadLink.download = newFilename;
            downloadLink.style.display = 'none';
            document.body.appendChild(downloadLink);
            downloadLink.click();
            document.body.removeChild(downloadLink);
        })
        .catch(error => {
            console.error('An error occurred:', error);
        });
});