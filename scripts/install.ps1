$ErrorActionPreference = 'Stop'

if (-not [Environment]::Is64BitOperatingSystem) {
    throw 'mssh: this installer supports Windows x86_64'
}

$repo = 'KirillShakhov/HopMux'
$installDir = if ($env:MSSH_INSTALL_DIR) { $env:MSSH_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\mssh' }
$configDir = if ($env:MSSH_CONFIG_DIR) { $env:MSSH_CONFIG_DIR } else { Join-Path $env:APPDATA 'mssh' }
$releaseUrl = if ($env:MSSH_RELEASE_URL) { $env:MSSH_RELEASE_URL } else { "https://github.com/$repo/releases/latest/download/mssh-windows-x86_64.exe" }
$exampleUrl = if ($env:MSSH_EXAMPLE_URL) { $env:MSSH_EXAMPLE_URL } else { "https://raw.githubusercontent.com/$repo/main/examples/config.toml" }

New-Item -ItemType Directory -Force -Path $installDir, $configDir | Out-Null
$binary = Join-Path $installDir 'mssh.exe'
$download = Join-Path $installDir ('.mssh-download-' + [guid]::NewGuid().ToString('N') + '.exe')
$backup = Join-Path $installDir ('.mssh-backup-' + [guid]::NewGuid().ToString('N') + '.exe')
$config = Join-Path $configDir 'config.toml'
$configDownload = Join-Path $configDir ('.config-download-' + [guid]::NewGuid().ToString('N') + '.toml')

try {
    if (-not (Test-Path -LiteralPath $config)) {
        Invoke-WebRequest -Uri $exampleUrl -OutFile $configDownload
        if ((Get-Item -LiteralPath $configDownload).Length -eq 0) { throw 'mssh: downloaded example config is empty' }
        if (-not (Test-Path -LiteralPath $config)) {
            Move-Item -LiteralPath $configDownload -Destination $config
            Write-Host "Created $config from the main branch example"
        }
    }

    Invoke-WebRequest -Uri $releaseUrl -OutFile $download
    if ((Get-Item -LiteralPath $download).Length -eq 0) { throw 'mssh: downloaded binary is empty' }
    & $download --version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'mssh: downloaded binary failed its version check' }

    if (Test-Path -LiteralPath $binary) {
        Move-Item -LiteralPath $binary -Destination $backup
    }
    try {
        Move-Item -LiteralPath $download -Destination $binary
    } catch {
        if (Test-Path -LiteralPath $backup) {
            Move-Item -LiteralPath $backup -Destination $binary
        }
        throw
    }
    Write-Host "Installed $(& $binary --version) to $binary"

    if ($env:MSSH_SKIP_PATH_UPDATE -ne '1') {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        $entries = @($userPath -split ';' | Where-Object { $_ })
        if ($entries -notcontains $installDir) {
            [Environment]::SetEnvironmentVariable('Path', (($entries + $installDir) -join ';'), 'User')
            Write-Host 'Open a new terminal to use mssh from any directory.'
        }
    }
} finally {
    foreach ($path in @($download, $configDownload)) {
        if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force }
    }
    if (Test-Path -LiteralPath $backup) {
        if (Test-Path -LiteralPath $binary) {
            Remove-Item -LiteralPath $backup -Force
        } else {
            Write-Warning "Previous mssh binary retained at $backup"
        }
    }
}
